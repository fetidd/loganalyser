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

pub enum Clause {
    Condition(Filterable),
    And(Vec<Clause>),
    Or(Vec<Clause>),
}

pub struct EventFilter {
    clause: Option<Clause>,
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

    pub fn clause(&self) -> Option<&Clause> {
        self.clause.as_ref()
    }

    pub fn with_data(mut self, field: &str, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = Cmp::Json(field.into(), Box::new(cmp.map(Into::into)));
        if let Some(clause) = self.clause {
            match clause {
                Clause::Condition(filterable) => {
                    self.clause = Some(Clause::And(vec![
                        Clause::Condition(filterable),
                        Clause::Condition(Filterable::Data(cmp)),
                    ]))
                }
                Clause::And(mut clauses) => {
                    clauses.push(Clause::Condition(Filterable::Data(cmp)));
                    self.clause = Some(Clause::And(clauses));
                }
                _ => panic!("with_ methods cannot OR"),
            }
        } else {
            self.clause = Some(Clause::Condition(Filterable::Data(cmp)));
        }
        self
    }

    pub fn with_timestamp(mut self, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = cmp.map(Into::into);
        if let Some(clause) = self.clause {
            match clause {
                Clause::Condition(filterable) => {
                    self.clause = Some(Clause::And(vec![
                        Clause::Condition(filterable),
                        Clause::Condition(Filterable::Timestamp(cmp)),
                    ]))
                }
                Clause::And(mut clauses) => {
                    clauses.push(Clause::Condition(Filterable::Timestamp(cmp)));
                    self.clause = Some(Clause::And(clauses));
                }
                _ => panic!("with_ methods cannot OR"),
            }
        } else {
            self.clause = Some(Clause::Condition(Filterable::Timestamp(cmp)));
        }
        self
    }

    pub fn with_id(mut self, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = cmp.map(Into::into);
        if let Some(clause) = self.clause {
            match clause {
                Clause::Condition(filterable) => {
                    self.clause = Some(Clause::And(vec![
                        Clause::Condition(filterable),
                        Clause::Condition(Filterable::Id(cmp)),
                    ]))
                }
                Clause::And(mut clauses) => {
                    clauses.push(Clause::Condition(Filterable::Id(cmp)));
                    self.clause = Some(Clause::And(clauses));
                }
                _ => panic!("with_ methods cannot OR"),
            }
        } else {
            self.clause = Some(Clause::Condition(Filterable::Id(cmp)));
        }
        self
    }

    pub fn with_parent_id(mut self, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = cmp.map(Into::into);
        if let Some(clause) = self.clause {
            match clause {
                Clause::Condition(filterable) => {
                    self.clause = Some(Clause::And(vec![
                        Clause::Condition(filterable),
                        Clause::Condition(Filterable::ParentId(cmp)),
                    ]))
                }
                Clause::And(mut clauses) => {
                    clauses.push(Clause::Condition(Filterable::ParentId(cmp)));
                    self.clause = Some(Clause::And(clauses));
                }
                _ => panic!("with_ methods cannot OR"),
            }
        } else {
            self.clause = Some(Clause::Condition(Filterable::ParentId(cmp)));
        }
        self
    }

    pub fn with_duration(mut self, cmp: Cmp<impl Into<u64>>) -> Self {
        let cmp = cmp.map(Into::into);
        if let Some(clause) = self.clause {
            match clause {
                Clause::Condition(filterable) => {
                    self.clause = Some(Clause::And(vec![
                        Clause::Condition(filterable),
                        Clause::Condition(Filterable::Duration(cmp)),
                    ]))
                }
                Clause::And(mut clauses) => {
                    clauses.push(Clause::Condition(Filterable::Duration(cmp)));
                    self.clause = Some(Clause::And(clauses));
                }
                _ => panic!("with_ methods cannot OR"),
            }
        } else {
            self.clause = Some(Clause::Condition(Filterable::Duration(cmp)));
        }
        self
    }
}

impl Default for EventFilter {
    fn default() -> Self {
        Self { clause: None }
    }
}
