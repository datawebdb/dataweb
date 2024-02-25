use arrow_schema::DataType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct EntityDeclaration {
    pub name: String,
    pub information: Vec<InformationDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct InformationDeclaration {
    pub name: String,
    pub arrow_dtype: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ResolvedEntityDeclaration {
    pub name: String,
    pub information: Vec<ResolvedInformationDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ResolvedInformationDeclaration {
    pub name: String,
    pub arrow_dtype: DataType,
}
