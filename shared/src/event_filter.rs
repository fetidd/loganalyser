#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Condition(Predicate),
    And(Vec<Expr>),
    Or(Vec<Expr>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    Data(Cmp<String>),
    Timestamp(Cmp<String>),
    Id(Cmp<String>),
    ParentId(Cmp<String>),
    Duration(Cmp<i64>),
    Name(Cmp<String>),
    RawLine(Cmp<String>),
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

#[derive(Default)]
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

    pub fn with_raw_line(self, cmp: Cmp<impl Into<String>>) -> Self {
        self.and_condition(Predicate::RawLine(cmp.map(Into::into)))
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

    fn pred(expr: Expr) -> Predicate {
        let Expr::Condition(p) = expr else { panic!("expected Condition, got {expr:?}") };
        p
    }

    #[test]
    fn test_new_filter_has_no_expr() {
        assert!(Filter::new().expr().is_none());
    }

    #[rstest]
    #[case(name(Like("http%")),          Predicate::Name(Like("http%".into())))]
    #[case(id(Eq("abc")),                Predicate::Id(Eq("abc".into())))]
    #[case(timestamp(Gt("2025-01-01")),  Predicate::Timestamp(Gt("2025-01-01".into())))]
    #[case(duration(Lt(500)), Predicate::Duration(Lt(500)))]
    #[case(parent_id(Eq("pid")),         Predicate::ParentId(Eq("pid".into())))]
    fn test_free_fn_predicate_value(#[case] expr: Expr, #[case] expected: Predicate) {
        assert_eq!(pred(expr), expected);
    }

    #[test]
    fn test_data_free_fn_wraps_in_json() {
        let expr = data("status", Eq("200"));
        let Predicate::Data(Cmp::Json(field, inner)) = pred(expr) else { panic!("expected Data(Json(…))") };
        assert_eq!(field, "status");
        assert_eq!(*inner, Eq("200".into()));
    }

    #[rstest]
    #[case(Filter::new().with_name(Like("http%")),         Predicate::Name(Like("http%".into())))]
    #[case(Filter::new().with_id(Eq("abc")),               Predicate::Id(Eq("abc".into())))]
    #[case(Filter::new().with_timestamp(Gte("2025-01-01")),Predicate::Timestamp(Gte("2025-01-01".into())))]
    #[case(Filter::new().with_duration(Lte(999)),           Predicate::Duration(Lte(999)))]
    #[case(Filter::new().with_parent_id(Eq("pid")),        Predicate::ParentId(Eq("pid".into())))]
    fn test_single_with_value(#[case] f: Filter, #[case] expected: Predicate) {
        let Some(Expr::Condition(p)) = f.expr() else { panic!("expected Condition") };
        assert_eq!(*p, expected);
    }

    #[test]
    fn test_with_data_wraps_in_json() {
        let f = Filter::new().with_data("status", Eq("200"));
        let Some(Expr::Condition(Predicate::Data(Cmp::Json(field, inner)))) = f.expr() else { panic!("expected Data(Json(…))") };
        assert_eq!(field, "status");
        assert_eq!(**inner, Eq("200".into()));
    }

    #[test]
    fn test_two_withs_accumulate_as_flat_and() {
        let f = Filter::new().with_id(Eq("abc")).with_timestamp(Gt("2025-01-01"));
        let Some(Expr::And(exprs)) = f.expr() else { panic!("expected And") };
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0], Expr::Condition(Predicate::Id(Eq("abc".into()))));
        assert_eq!(exprs[1], Expr::Condition(Predicate::Timestamp(Gt("2025-01-01".into()))));
    }

    #[test]
    fn test_three_withs_remain_flat() {
        let f = Filter::new().with_id(Eq("abc")).with_timestamp(Gt("2025-01-01")).with_duration(Lt(1000));
        let Some(Expr::And(exprs)) = f.expr() else { panic!("expected And") };
        assert_eq!(exprs.len(), 3);
        assert_eq!(exprs[2], Expr::Condition(Predicate::Duration(Lt(1000))));
    }

    #[test]
    fn test_filter_from_expr_preserves_value() {
        let f: Filter = id(Eq("abc")).into();
        let Some(Expr::Condition(p)) = f.expr() else { panic!() };
        assert_eq!(*p, Predicate::Id(Eq("abc".into())));
    }

    #[test]
    fn test_filter_from_or_preserves_children() {
        let f: Filter = or([id(Eq("a")), timestamp(Gt("2025-01-01"))]).into();
        let Some(Expr::Or(exprs)) = f.expr() else { panic!("expected Or") };
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0], Expr::Condition(Predicate::Id(Eq("a".into()))));
        assert_eq!(exprs[1], Expr::Condition(Predicate::Timestamp(Gt("2025-01-01".into()))));
    }

    #[test]
    #[should_panic(expected = "with_ methods cannot OR")]
    fn test_with_panics_on_or_filter() {
        let f: Filter = or([id(Eq("a")), id(Eq("b"))]).into();
        f.with_timestamp(Gt("2025-01-01"));
    }

    #[test]
    fn test_and_free_fn_children() {
        let Expr::And(exprs) = and([id(Eq("a")), timestamp(Gt("b")), duration(Lt(100))]) else { panic!("expected And") };
        assert_eq!(exprs.len(), 3);
        assert_eq!(exprs[0], Expr::Condition(Predicate::Id(Eq("a".into()))));
        assert_eq!(exprs[1], Expr::Condition(Predicate::Timestamp(Gt("b".into()))));
        assert_eq!(exprs[2], Expr::Condition(Predicate::Duration(Lt(100))));
    }

    #[test]
    fn test_or_free_fn_children() {
        let Expr::Or(exprs) = or([id(Eq("a")), name(Like("%b%"))]) else { panic!("expected Or") };
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0], Expr::Condition(Predicate::Id(Eq("a".into()))));
        assert_eq!(exprs[1], Expr::Condition(Predicate::Name(Like("%b%".into()))));
    }

    // ── Cmp::map ─────────────────────────────────────────────────────────────

    #[test]
    fn test_cmp_map_scalar() {
        let up = |s: &str| s.to_uppercase();
        assert_eq!(Eq("a").map(up), Eq("A".to_string()));
        assert_eq!(Gt("b").map(up), Gt("B".to_string()));
        assert_eq!(Lt("c").map(up), Lt("C".to_string()));
        assert_eq!(Gte("d").map(up), Gte("D".to_string()));
        assert_eq!(Lte("e").map(up), Lte("E".to_string()));
        assert_eq!(Like("f").map(up), Like("F".to_string()));
    }

    #[test]
    fn test_cmp_map_in() {
        assert_eq!(In(vec!["a", "b"]).map(|s: &str| s.to_uppercase()), In(vec!["A".to_string(), "B".to_string()]));
    }

    #[test]
    fn test_cmp_map_json_maps_inner_preserves_key() {
        let cmp: Cmp<&str> = Json("field".into(), Box::new(Eq("value")));
        assert_eq!(cmp.map(|s: &str| s.to_uppercase()), Json("field".into(), Box::new(Eq("VALUE".to_string()))));
    }
}
