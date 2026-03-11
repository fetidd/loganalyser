#[derive(Clone, Debug)]
pub enum Cmp<T> {
    Eq(T),
    Gt(T),
    Lt(T),
    Gte(T),
    Lte(T),
    Like(T),
    In(Vec<T>),
    Json(String, Box<Cmp<T>>),
}

impl<T> Cmp<T> {
    pub fn map<U>(self, f: impl Fn(T) -> U) -> Cmp<U> {
        match self {
            Cmp::Eq(v) => Cmp::Eq(f(v)),
            Cmp::Gt(v) => Cmp::Gt(f(v)),
            Cmp::Lt(v) => Cmp::Lt(f(v)),
            Cmp::Gte(v) => Cmp::Gte(f(v)),
            Cmp::Lte(v) => Cmp::Lte(f(v)),
            Cmp::Like(v) => Cmp::Like(f(v)),
            Cmp::In(vals) => Cmp::In(vals.into_iter().map(f).collect()),
            Cmp::Json(k, inner) => Cmp::Json(k, Box::new(inner.map(f))),
        }
    }
}

pub struct EventFilter {
    filters: Vec<Filterable>,
}

pub enum Filterable {
    Data(Cmp<String>),
    Timestamp(Cmp<String>),
    Id(Cmp<String>),
    ParentId(Cmp<String>),
    Duration(Cmp<u64>),
}

impl EventFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn filters(&self) -> &Vec<Filterable> {
        &self.filters
    }

    pub fn with_data(mut self, field: &str, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = Cmp::Json(field.into(), Box::new(cmp.map(Into::into)));
        self.filters.push(Filterable::Data(cmp));
        self
    }

    pub fn with_timestamp(mut self, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = cmp.map(Into::into);
        self.filters.push(Filterable::Timestamp(cmp));
        self
    }

    pub fn with_id(mut self, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = cmp.map(Into::into);
        self.filters.push(Filterable::Id(cmp));
        self
    }

    pub fn with_parent_id(mut self, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = cmp.map(Into::into);
        self.filters.push(Filterable::ParentId(cmp));
        self
    }

    pub fn with_duration(mut self, cmp: Cmp<impl Into<u64>>) -> Self {
        let cmp = cmp.map(Into::into);
        self.filters.push(Filterable::Duration(cmp));
        self
    }
}

impl Default for EventFilter {
    fn default() -> Self {
        Self { filters: vec![] }
    }
}
