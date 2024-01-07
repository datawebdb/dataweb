use crate::error::{MeshError, Result};
use crate::model::{
    data_stores::{DataConnection, DataSource},
    query::{
        FlightStream, NewFlightStream, NewQueryTask, QueryOriginationInfo, QueryRequest, QueryTask,
        QueryTaskRemote, QueryTaskRemoteStatus, QueryTaskStatus, SubstitutionBlocks,
    },
    relay::Relay,
};

use crate::schema;
use diesel::result::DatabaseErrorKind;
use diesel::{insert_into, prelude::*, update};
use diesel_async::RunQueryDsl;

use uuid::Uuid;

use super::PgDb;

impl<'a> PgDb<'a> {
    pub async fn create_query_request(
        &mut self,
        local_id_val: &Uuid,
        relay_id_val: &Uuid,
        originator_request_id_val: &Uuid,
        sql_val: &str,
        blocks: &SubstitutionBlocks,
        origin_info_val: &QueryOriginationInfo,
    ) -> Result<QueryRequest> {
        use schema::query_request::dsl::*;
        let r: Result<QueryRequest, diesel::result::Error> = insert_into(query_request)
            .values((
                id.eq(local_id_val),
                relay_id.eq(relay_id_val),
                sql.eq(sql_val),
                originator_request_id.eq(originator_request_id_val),
                substitution_blocks.eq(blocks),
                origin_info.eq(origin_info_val),
            ))
            .get_result(&mut self.con)
            .await;
        // Check if the error is due to violating unique global query uuid constraint.
        // If yes, return the already existing QueryRequest wrapped in error.
        // Otherwise return Result as is.
        match &r {
            Ok(_) => (),
            Err(e) => {
                if let diesel::result::Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _) =
                    e
                {
                    let already_recieved_request: QueryRequest = query_request
                        .filter(originator_request_id.eq(originator_request_id_val))
                        .get_result(&mut self.con)
                        .await?;
                    return Err(MeshError::DuplicateQueryRequest(Box::new(
                        already_recieved_request,
                    )));
                }
            }
        }
        Ok(r?)
    }

    pub async fn get_all_flight_streams(
        &mut self,
        remote_tasks: &[QueryTaskRemote],
    ) -> Result<Vec<(QueryTaskRemote, FlightStream)>> {
        use schema::incoming_flight_streams::dsl as flight;
        use schema::query_task_remote::dsl::*;

        Ok(flight::incoming_flight_streams
            .inner_join(query_task_remote)
            .filter(id.eq_any(remote_tasks.iter().map(|obj| obj.id)))
            .select((QueryTaskRemote::as_select(), FlightStream::as_select()))
            .get_results(&mut self.con)
            .await?)
    }

    pub async fn check_if_request_already_received(
        &mut self,
        originator_request_id_val: &Uuid,
    ) -> Result<QueryRequest> {
        use schema::query_request::dsl::*;
        Ok(query_request
            .filter(
                originator_request_id
                    .eq(originator_request_id_val)
                    .or(id.eq(originator_request_id_val)),
            )
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn get_query_request(
        &mut self,
        id_val: Uuid,
    ) -> Result<Option<(QueryRequest, Vec<QueryTask>, Vec<QueryTaskRemote>)>> {
        use schema::query_request::dsl as req;
        use schema::query_task::dsl as task;
        use schema::query_task_remote::dsl as remote;

        let joined_tasks: Vec<(QueryRequest, QueryTask)> = req::query_request
            .inner_join(task::query_task)
            .select((QueryRequest::as_select(), QueryTask::as_select()))
            .filter(req::id.eq(id_val))
            .get_results(&mut self.con)
            .await?;

        let joined_remote_tasks: Vec<(QueryRequest, QueryTaskRemote)> = req::query_request
            .inner_join(remote::query_task_remote)
            .select((QueryRequest::as_select(), QueryTaskRemote::as_select()))
            .filter(req::id.eq(id_val))
            .get_results(&mut self.con)
            .await?;

        let mut request = None;
        let mut tasks = Vec::with_capacity(joined_tasks.len());
        let mut remote_tasks = Vec::with_capacity(joined_remote_tasks.len());

        for (i, (req, task)) in joined_tasks.into_iter().enumerate() {
            if i == 0 {
                request = Some(req);
            }
            tasks.push(task);
        }

        for (req, task) in joined_remote_tasks {
            if request.is_none() {
                request = Some(req);
            }
            remote_tasks.push(task);
        }

        match request {
            Some(req) => Ok(Some((req, tasks, remote_tasks))),
            None => Ok(None),
        }
    }

    pub async fn create_query_tasks(&mut self, vals: &Vec<NewQueryTask>) -> Result<Vec<QueryTask>> {
        use schema::query_task::dsl::*;
        Ok(insert_into(query_task)
            .values(vals)
            .get_results(&mut self.con)
            .await?)
    }

    pub async fn create_remote_query_tasks(
        &mut self,
        vals: &Vec<QueryTaskRemote>,
    ) -> Result<Vec<QueryTaskRemote>> {
        use schema::query_task_remote::dsl::*;
        Ok(insert_into(query_task_remote)
            .values(vals)
            .get_results(&mut self.con)
            .await?)
    }

    pub async fn upsert_flight_stream(&mut self, flight: &NewFlightStream) -> Result<()> {
        use schema::incoming_flight_streams::dsl::*;
        insert_into(incoming_flight_streams)
            .values(flight)
            .on_conflict(flight_id)
            .do_update()
            .set(flight)
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    pub async fn get_query_task(
        &mut self,
        id_val: Uuid,
    ) -> Result<(DataConnection, DataSource, QueryTask, QueryRequest, Relay)> {
        use schema::data_connection::dsl as con;
        use schema::data_source::dsl as source;
        use schema::query_request::dsl as request;
        use schema::query_task::dsl::*;
        use schema::relays::dsl as relays;
        Ok(query_task
            .inner_join(source::data_source.inner_join(con::data_connection))
            .inner_join(request::query_request.inner_join(relays::relays))
            .select((
                DataConnection::as_select(),
                DataSource::as_select(),
                QueryTask::as_select(),
                QueryRequest::as_select(),
                Relay::as_select(),
            ))
            .filter(id.eq(id_val))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn get_remote_query_task(
        &mut self,
        id_val: Uuid,
    ) -> Result<(QueryTaskRemote, Relay)> {
        use schema::query_task_remote::dsl::*;
        use schema::relays::dsl as relay;
        Ok(query_task_remote
            .inner_join(relay::relays)
            .select((QueryTaskRemote::as_select(), Relay::as_select()))
            .filter(id.eq(id_val))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn update_task_status(
        &mut self,
        id_val: Uuid,
        status_val: QueryTaskStatus,
    ) -> Result<()> {
        use schema::query_task::dsl::*;
        update(query_task)
            .filter(id.eq(id_val))
            .set(status.eq(status_val))
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    pub async fn update_remote_task_status(
        &mut self,
        id_val: Uuid,
        status_val: QueryTaskRemoteStatus,
    ) -> Result<()> {
        use schema::query_task_remote::dsl::*;
        update(query_task_remote)
            .filter(id.eq(id_val))
            .set(status.eq(status_val))
            .execute(&mut self.con)
            .await?;
        Ok(())
    }
}
