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

pub enum Expr {
    Condition(Predicate),
    And(Vec<Expr>),
    Or(Vec<Expr>),
}

pub enum Predicate {
    Data(Cmp<String>),
    Timestamp(Cmp<String>),
    Id(Cmp<String>),
    ParentId(Cmp<String>),
    Duration(Cmp<i64>),
}

pub fn id(c: Cmp<impl Into<String>>) -> Expr {
    Expr::Condition(Predicate::Id(c.map(Into::into)))
}

pub fn timestamp(c: Cmp<impl Into<String>>) -> Expr {
    Expr::Condition(Predicate::Timestamp(c.map(Into::into)))
}

pub fn duration(c: Cmp<impl Into<i64>>) -> Expr {
    Expr::Condition(Predicate::Duration(c.map(Into::into)))
}

pub fn data(field: &str, c: Cmp<impl Into<String>>) -> Expr {
    let cmp = Cmp::Json(field.into(), Box::new(c.map(Into::into)));
    Expr::Condition(Predicate::Data(cmp))
}

pub fn parent_id(c: Cmp<impl Into<String>>) -> Expr {
    Expr::Condition(Predicate::ParentId(c.map(Into::into)))
}

pub fn and(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    Expr::And(exprs.into_iter().collect())
}

pub fn or(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    Expr::Or(exprs.into_iter().collect())
}

pub struct Filter {
    expr: Option<Expr>,
}

impl Filter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expr(&self) -> Option<&Expr> {
        self.expr.as_ref()
    }

    fn and_condition(mut self, predicate: Predicate) -> Self {
        let new_expr = Expr::Condition(predicate);
        self.expr = Some(match self.expr {
            None => new_expr,
            Some(Expr::Condition(existing)) => Expr::And(vec![Expr::Condition(existing), new_expr]),
            Some(Expr::And(mut exprs)) => {
                exprs.push(new_expr);
                Expr::And(exprs)
            }
            Some(Expr::Or(_)) => panic!("with_ methods cannot OR"),
        });
        self
    }

    pub fn with_data(self, field: &str, cmp: Cmp<impl Into<String>>) -> Self {
        let cmp = Cmp::Json(field.into(), Box::new(cmp.map(Into::into)));
        self.and_condition(Predicate::Data(cmp))
    }

    pub fn with_timestamp(self, cmp: Cmp<impl Into<String>>) -> Self {
        self.and_condition(Predicate::Timestamp(cmp.map(Into::into)))
    }

    pub fn with_id(self, cmp: Cmp<impl Into<String>>) -> Self {
        self.and_condition(Predicate::Id(cmp.map(Into::into)))
    }

    pub fn with_parent_id(self, cmp: Cmp<impl Into<String>>) -> Self {
        self.and_condition(Predicate::ParentId(cmp.map(Into::into)))
    }

    pub fn with_duration(self, cmp: Cmp<impl Into<i64>>) -> Self {
        self.and_condition(Predicate::Duration(cmp.map(Into::into)))
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self { expr: None }
    }
}

impl From<Expr> for Filter {
    fn from(expr: Expr) -> Self {
        Self { expr: Some(expr) }
    }
}
