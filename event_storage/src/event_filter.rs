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

#[derive(Clone)]
pub enum Expr {
    Condition(Predicate),
    And(Vec<Expr>),
    Or(Vec<Expr>),
}

#[derive(Clone)]
pub enum Predicate {
    Data(Cmp<String>),
    Timestamp(Cmp<String>),
    Id(Cmp<String>),
    ParentId(Cmp<String>),
    Duration(Cmp<i64>),
    Name(Cmp<String>),
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

pub fn name(c: Cmp<impl Into<String>>) -> Expr {
    Expr::Condition(Predicate::Name(c.map(Into::into)))
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

    pub fn with_name(self, cmp: Cmp<impl Into<String>>) -> Self {
        self.and_condition(Predicate::Name(cmp.map(Into::into)))
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

#[cfg(test)]
mod tests {
    use super::Cmp::*;
    use super::*;
    use rstest::rstest;

    #[test]
    fn test_new_filter_has_no_expr() {
        assert!(Filter::new().expr().is_none());
    }

    #[test]
    fn test_name_free_fn_produces_name_condition() {
        let expr = name(Like("http%"));
        assert!(matches!(expr, Expr::Condition(Predicate::Name(_))));
    }

    #[test]
    fn test_with_name_produces_condition() {
        let f = Filter::new().with_name(Like("http%"));
        assert!(matches!(f.expr(), Some(Expr::Condition(Predicate::Name(_)))));
    }

    #[test]
    fn test_with_name_combined_with_other_produces_and() {
        let f = Filter::new().with_name(Like("http%")).with_timestamp(Gt("2025-01-01"));
        assert!(matches!(f.expr(), Some(Expr::And(v)) if v.len() == 2));
    }

    #[test]
    fn test_single_with_produces_condition() {
        let f = Filter::new().with_id(Eq("abc"));
        assert!(matches!(f.expr(), Some(Expr::Condition(Predicate::Id(_)))));
    }

    #[test]
    fn test_two_withs_produce_and() {
        let f = Filter::new().with_id(Eq("abc")).with_timestamp(Gt("2025-01-01"));
        assert!(matches!(f.expr(), Some(Expr::And(v)) if v.len() == 2));
    }

    #[test]
    fn test_three_withs_produce_flat_and() {
        // third condition should extend the existing And, not nest it
        let f = Filter::new()
            .with_id(Eq("abc"))
            .with_timestamp(Gt("2025-01-01"))
            .with_duration(Lt(1000));
        assert!(matches!(f.expr(), Some(Expr::And(v)) if v.len() == 3));
    }

    #[test]
    fn test_filter_from_expr_wraps_correctly() {
        let f: Filter = id(Eq("abc")).into();
        assert!(matches!(f.expr(), Some(Expr::Condition(Predicate::Id(_)))));
    }

    #[test]
    fn test_filter_from_or_expr() {
        let f: Filter = or([id(Eq("a")), timestamp(Gt("2025-01-01"))]).into();
        assert!(matches!(f.expr(), Some(Expr::Or(v)) if v.len() == 2));
    }

    #[test]
    #[should_panic(expected = "with_ methods cannot OR")]
    fn test_with_panics_on_or_filter() {
        let f: Filter = or([id(Eq("a")), id(Eq("b"))]).into();
        f.with_timestamp(Gt("2025-01-01"));
    }

    #[rstest]
    #[case(and([id(Eq("a")), timestamp(Gt("b"))]), 2)]
    #[case(and([id(Eq("a")), timestamp(Gt("b")), duration(Lt(100))]), 3)]
    fn test_and_free_fn_child_count(#[case] expr: Expr, #[case] expected_len: usize) {
        assert!(matches!(expr, Expr::And(v) if v.len() == expected_len));
    }

    #[rstest]
    #[case(or([id(Eq("a")), timestamp(Gt("b"))]), 2)]
    #[case(or([id(Eq("a")), timestamp(Gt("b")), duration(Lt(100))]), 3)]
    fn test_or_free_fn_child_count(#[case] expr: Expr, #[case] expected_len: usize) {
        assert!(matches!(expr, Expr::Or(v) if v.len() == expected_len));
    }

    #[rstest]
    #[case(Eq("a"), "a")]
    #[case(Gt("b"), "b")]
    #[case(Lt("c"), "c")]
    #[case(Gte("d"), "d")]
    #[case(Lte("e"), "e")]
    #[case(Like("f"), "f")]
    fn test_cmp_map_scalar(#[case] cmp: Cmp<&str>, #[case] expected: &str) {
        let mapped = cmp.map(|s| s.to_uppercase());
        let val = match mapped {
            Eq(v) | Gt(v) | Lt(v) | Gte(v) | Lte(v) | Like(v) => v,
            _ => panic!("unexpected variant"),
        };
        assert_eq!(val, expected.to_uppercase());
    }

    #[test]
    fn test_cmp_map_in() {
        let mapped = In(vec!["a", "b"]).map(|s: &str| s.to_uppercase());
        assert!(matches!(mapped, In(v) if v == vec!["A", "B"]));
    }

    #[test]
    fn test_cmp_map_json_maps_inner() {
        let cmp: Cmp<&str> = Json("field".into(), Box::new(Eq("value")));
        let mapped = cmp.map(|s: &str| s.to_uppercase());
        assert!(matches!(mapped, Json(k, inner) if k == "field" && matches!(*inner, Eq(ref v) if v == "VALUE")));
    }
}
