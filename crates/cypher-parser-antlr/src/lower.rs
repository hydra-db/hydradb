//! Cypher 25 generated-tree adapter.
//!
//! This module is intentionally private: only this ANTLR adapter knows about
//! generated parser types. Everything it returns is owned by
//! `hydradb-cypher-ast`.
//!
//! Lowering walks the validated parse tree itself. Every decision about what a
//! query means is taken from the grammar's structure -- which rule a node is,
//! which tokens sit between its children -- never by re-reading the source
//! text, so precedence, comments, string escapes and keyword spelling are the
//! grammar's and not an approximation of it.
//!
//! The typed surface is deliberately narrower than the grammar. A construct is
//! lowered only once the engine behind the shared AST executes it; anything
//! else stops lowering and the statement is kept as a [`SyntaxFragment`]
//! naming the clause it stopped in, so the failure lands in the same
//! [`QueryFailureReason`] bucket the legacy engine reports.

use antlr4_runtime::{RuleNodeView, Token as _};
use hydradb_cypher_ast::{
    BinaryOperator, CallClause, Clause, CreateClause, CypherDialect, CypherVersion, DeleteClause, Direction, Document, ElementTest,
    Expression, FilterClause, FloatSize, ForPosition, ForeachClause, Identifier, IntegerSize, IntervalField, IntervalQualifier,
    IterationNodes, LabelExpression, LetClause, Literal, MatchClause, MatchMode, MergeClause, MutationClause, NodePattern, NullOrdering,
    OrderByAndPageClause, OrderDirection, PathAlternatives, PathLength, PathMode, PathOperand, PathPattern, PathPrefix, PathSearch,
    PathSearchKind, PatternElement, Projection, ProjectionItems, PropertiesUpdate, Query, QueryConjunction, QueryFailureReason,
    RelationshipPattern, RemoveClause, RemoveItem, ReturnClause, SetClause, SetItem, SetQuantifier, SortItem, Statement, SyntaxFragment,
    TemporalLiteralKind, UnaryOperator, UnionArm, UnwindClause, ValueKeyword, ValueType, ValueTypeKind, WithClause,
};

use crate::generated::cypher25_parser as grammar;
use crate::Cypher25ValidatedTree;

pub(super) fn lower_cypher25(source: &str, syntax: &Cypher25ValidatedTree) -> Document {
    let root = Cst(syntax.tree().rule_node());
    let mut lowering = Lowering {
        source,
        context: QueryFailureReason::UntypedStatement,
        open_query: false,
        rejection: None,
    };
    let statement = match lowering.statements(root) {
        Ok(query) => Statement::Query(query),
        Err(reason) => Statement::Syntax(match lowering.rejection {
            Some(status) => SyntaxFragment::rejected(source, reason, status),
            None => SyntaxFragment::unsupported(source, reason),
        }),
    };
    Document::new(CypherDialect::Neo4j(CypherVersion::V25), vec![statement])
}

/// Why lowering stopped: the bucket of the clause it was reading.
type Lowered<T> = Result<T, QueryFailureReason>;

fn has_primitive_result(node: Cst<'_>) -> bool {
    match node.rule() {
        grammar::RULE_REGULAR_QUERY => node
            .first(grammar::RULE_UNION)
            .or_else(|| node.first(grammar::RULE_WHEN))
            .is_some_and(has_primitive_result),
        grammar::RULE_UNION => node.rules(grammar::RULE_SINGLE_QUERY).all(has_primitive_result),
        grammar::RULE_WHEN => node
            .rules(grammar::RULE_WHEN_BRANCH)
            .chain(node.rules(grammar::RULE_ELSE_BRANCH))
            .all(|branch| branch.first(grammar::RULE_SINGLE_QUERY).is_some_and(has_primitive_result)),
        grammar::RULE_SINGLE_QUERY => match node.first(grammar::RULE_QUERY_WITH_LOCAL_DEFINITIONS) {
            Some(query) => query
                .first(grammar::RULE_NEXT_STATEMENT)
                .is_some_and(|next| next.rules(grammar::RULE_REGULAR_QUERY).all(has_primitive_result)),
            None => node.rules(grammar::RULE_CLAUSE).last().is_some_and(|clause| {
                clause.first(grammar::RULE_RETURN_CLAUSE).is_some() || clause.first(grammar::RULE_FINISH_CLAUSE).is_some()
            }),
        },
        _ => false,
    }
}

/// One rule node of the validated tree.
#[derive(Clone, Copy)]
struct Cst<'tree>(RuleNodeView<'tree>);

/// A direct child of a rule node, in source order.
enum Child<'tree> {
    Rule(Cst<'tree>),
    Token(i32, &'tree str),
}

impl<'tree> Cst<'tree> {
    fn rule(self) -> usize {
        self.0.rule_index()
    }

    /// Direct children in source order. A validated tree has no error nodes.
    fn children(self) -> impl Iterator<Item = Child<'tree>> {
        self.0.children().filter_map(|node| {
            if let Some(rule) = node.as_rule() {
                Some(Child::Rule(Cst(rule)))
            } else {
                node.as_terminal()
                    .map(|terminal| Child::Token(terminal.symbol().token_type(), terminal.text()))
            }
        })
    }

    /// Direct rule children of one rule kind, in source order.
    fn rules(self, rule: usize) -> impl Iterator<Item = Cst<'tree>> {
        self.children().filter_map(move |child| match child {
            Child::Rule(node) if node.rule() == rule => Some(node),
            _ => None,
        })
    }

    fn first(self, rule: usize) -> Option<Cst<'tree>> {
        self.rules(rule).next()
    }

    /// The child a validated tree guarantees, for rules whose grammar makes it
    /// required. Reaching `None` means the grammar changed under this adapter.
    fn required(self, rule: usize) -> Lowered<Cst<'tree>> {
        self.first(rule).ok_or(QueryFailureReason::UntypedStatement)
    }

    /// The single rule child of a wrapper rule such as `clause`.
    fn only_rule(self) -> Lowered<Cst<'tree>> {
        let mut rules = self.children().filter_map(|child| match child {
            Child::Rule(node) => Some(node),
            Child::Token(..) => None,
        });
        match (rules.next(), rules.next()) {
            (Some(rule), None) => Ok(rule),
            _ => Err(QueryFailureReason::UntypedStatement),
        }
    }

    fn has(self, token: i32) -> bool {
        self.children().any(|child| matches!(child, Child::Token(kind, _) if kind == token))
    }

    fn tokens(self) -> impl Iterator<Item = (i32, &'tree str)> {
        self.children().filter_map(|child| match child {
            Child::Token(kind, text) => Some((kind, text)),
            Child::Rule(_) => None,
        })
    }

    fn text(self) -> String {
        self.0.text()
    }
}

struct Lowering<'source> {
    source: &'source str,
    /// The clause being read, so a failure can be reported against it rather
    /// than as an untyped statement. Set on entering each clause.
    context: QueryFailureReason,
    /// Lowering an `EXISTS` subquery, whose clauses need not end in RETURN.
    open_query: bool,
    rejection: Option<hydradb_cypher_ast::GqlStatus>,
}

impl Lowering<'_> {
    /// Stop lowering, attributing the stop to the clause being read.
    fn refuse<T>(&self) -> Lowered<T> {
        Err(self.context)
    }

    fn statements(&mut self, root: Cst<'_>) -> Lowered<Query> {
        let mut statements = root.rules(grammar::RULE_STATEMENT);
        let (Some(statement), None) = (statements.next(), statements.next()) else {
            return Err(QueryFailureReason::UntypedStatement);
        };
        if let Some(command) = statement.first(grammar::RULE_COMMAND) {
            // Administration commands open with a keyword such as `CREATE`
            // or `SHOW`; the ones that name a clause keep its bucket.
            let keyword = command.text();
            let keyword = keyword
                .split(|character: char| !character.is_ascii_alphabetic())
                .next()
                .unwrap_or("");
            return Err(QueryFailureReason::for_clause_keyword(keyword).unwrap_or(QueryFailureReason::UntypedStatement));
        }
        self.query_with_local_definitions(statement.required(grammar::RULE_QUERY_WITH_LOCAL_DEFINITIONS)?)
    }

    fn query_with_local_definitions(&mut self, node: Cst<'_>) -> Lowered<Query> {
        let next = node.required(grammar::RULE_NEXT_STATEMENT)?;
        let queries: Vec<_> = next.rules(grammar::RULE_REGULAR_QUERY).collect();
        // Check the validated syntax tree before optional-feature lowering:
        // even an unsupported part must end in a primitive result (§9.2 SR7).
        if queries.len() > 1 && queries.iter().any(|part| !has_primitive_result(*part)) {
            self.rejection = Some(hydradb_cypher_ast::GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION);
            return Err(QueryFailureReason::Return);
        }
        if node.has(grammar::DEFINE) {
            return Err(QueryFailureReason::UntypedStatement);
        }
        let mut parts = Vec::with_capacity(queries.len());
        for query in &queries {
            let Some(union) = query.first(grammar::RULE_UNION) else {
                // A conditional `WHEN ... THEN ... ELSE` query.
                return Err(QueryFailureReason::UntypedStatement);
            };
            // A part before a NEXT need not end in RETURN as far as the
            // grammar goes; §9.2 Syntax Rule 7 is checked in lowering.
            let open_query = self.open_query;
            if queries.len() > 1 {
                self.open_query = true;
            }
            let part = self.union(union);
            self.open_query = open_query;
            parts.push(part?);
        }
        // §9.2 GQ20: `q1 NEXT q2` runs `q2` over the table `q1` returns. The
        // parts are joined by a NEXT clause, which engine lowering checks. A
        // composite part has no single result to hand on or to receive.
        if parts.len() > 1 && parts.iter().any(|part| !part.unions.is_empty()) {
            self.context = QueryFailureReason::Return;
            return self.refuse();
        }
        // §9.2 General Rule 1, GP17: the block's definitions amend the
        // working record every statement of the body starts from. Each is
        // the LET it means at the head of the first statement and each of
        // its arms, evaluated once; NEXT carries its value on (engine
        // lowering), rather than a copy evaluating it again later.
        let definitions = node
            .rules(grammar::RULE_BINDING_VARIABLE_DEFINITION)
            .map(|definition| self.value_variable_definition(definition.required(grammar::RULE_VALUE_VARIABLE_DEFINITION)?))
            .collect::<Lowered<Vec<_>>>()?;
        // Every arm, at any depth of nesting, starts from the same record.
        fn define(query: &mut Query, definitions: &[Clause]) {
            query.clauses = definitions.iter().cloned().chain(std::mem::take(&mut query.clauses)).collect();
            for arm in &mut query.unions {
                define(&mut arm.query, definitions);
            }
        }
        let mut parts = parts.into_iter();
        let mut query = parts.next().ok_or(QueryFailureReason::UntypedStatement)?;
        define(&mut query, &definitions);
        for part in parts {
            query.clauses.push(Clause::Next);
            query.clauses.extend(part.clauses);
        }
        Ok(query)
    }

    /// `VALUE x = e` (§10.3, GP05 to GP07): the LET of one item it means. A
    /// declared type is not read here.
    fn value_variable_definition(&mut self, node: Cst<'_>) -> Lowered<Clause> {
        self.context = QueryFailureReason::Return;
        Ok(Clause::Let(LetClause {
            bindings: vec![self.value_binding(node)?],
            definition: true,
        }))
    }

    fn value_binding(&mut self, node: Cst<'_>) -> Lowered<(Identifier, Expression)> {
        if node.first(grammar::RULE_TYPE).is_some() {
            return self.refuse();
        }
        let name = variable_name(node.required(grammar::RULE_VARIABLE)?);
        let value = self.expression(node.required(grammar::RULE_EXPRESSION)?)?;
        Ok((name, value))
    }

    /// `union`: arms joined by query conjunctions (§14.2), each arm after
    /// the first carrying the conjunction before it.
    fn union(&mut self, node: Cst<'_>) -> Lowered<Query> {
        let mut query: Option<Query> = None;
        // Arms a braced first operand brought with it: its own composite.
        let mut nested = 0;
        let mut conjunction = QueryConjunction::Union;
        let mut all = false;
        for child in node.children() {
            match child {
                Child::Token(kind, _) if [grammar::UNION, grammar::EXCEPT, grammar::INTERSECT, grammar::OTHERWISE].contains(&kind) => {
                    self.context = QueryFailureReason::Union;
                    all = false;
                    conjunction = match kind {
                        grammar::EXCEPT => QueryConjunction::Except,
                        grammar::INTERSECT => QueryConjunction::Intersect,
                        grammar::OTHERWISE => QueryConjunction::Otherwise,
                        _ => QueryConjunction::Union,
                    };
                }
                Child::Token(grammar::ALL, _) => all = true,
                Child::Token(..) => {}
                Child::Rule(arm) => {
                    let arm = self.single_query(arm)?;
                    match &mut query {
                        None => {
                            nested = arm.unions.len();
                            query = Some(arm);
                        }
                        Some(query) => query.unions.push(UnionArm {
                            conjunction,
                            all,
                            query: Box::new(arm),
                        }),
                    }
                }
            }
        }
        let query = query.ok_or(QueryFailureReason::UntypedStatement)?;
        // A braced composite as the first operand has no node of its own: its
        // arms join the outer ones. That is the same query only when every
        // arm joins the same way (a chain of one conjunction associates);
        // `{ A UNION B } UNION ALL C` is not `A UNION B UNION ALL C`.
        if nested > 0
            && query.unions[nested..]
                .iter()
                .any(|arm| (arm.conjunction, arm.all) != (query.unions[0].conjunction, query.unions[0].all))
        {
            return self.refuse();
        }
        Ok(query)
    }

    fn single_query(&mut self, node: Cst<'_>) -> Lowered<Query> {
        if node.has(grammar::LCURLY) {
            // `{ query }` as a query of its own (GQL §14.3, a nested query
            // specification) is that query. A `USE` in front of it chooses a
            // graph, which this engine does not.
            if node.first(grammar::RULE_USE_CLAUSE).is_some() {
                return self.refuse();
            }
            return self.query_with_local_definitions(node.required(grammar::RULE_QUERY_WITH_LOCAL_DEFINITIONS)?);
        }
        let clauses: Vec<_> = node.rules(grammar::RULE_CLAUSE).collect();
        let mut lowered = Vec::with_capacity(clauses.len());
        let mut returned = false;
        for clause in clauses {
            let clause = clause.only_rule()?;
            if returned {
                // Nothing follows RETURN in a typed query; name what does.
                return Err(self.clause_bucket(clause));
            }
            lowered.push(match clause.rule() {
                grammar::RULE_MATCH_CLAUSE => Clause::Match(self.match_clause(clause)?),
                grammar::RULE_WITH_CLAUSE => Clause::With(self.with_clause(clause)?),
                grammar::RULE_LET_CLAUSE => Clause::Let(self.let_clause(clause)?),
                grammar::RULE_FILTER_CLAUSE => Clause::Filter(self.filter_clause(clause)?),
                grammar::RULE_UNWIND_CLAUSE => Clause::Unwind(self.unwind_clause(clause)?),
                grammar::RULE_FOR_LIST_CLAUSE => Clause::Unwind(self.for_list_clause(clause)?),
                grammar::RULE_CREATE_CLAUSE => Clause::Create(self.create_clause(clause)?),
                grammar::RULE_MERGE_CLAUSE => Clause::Merge(self.merge_clause(clause)?),
                grammar::RULE_SET_CLAUSE => Clause::Set(self.set_clause(clause)?),
                grammar::RULE_REMOVE_CLAUSE => Clause::Remove(self.remove_clause(clause)?),
                grammar::RULE_DELETE_CLAUSE => Clause::Delete(self.delete_clause(clause)?),
                grammar::RULE_FOREACH_CLAUSE => Clause::Foreach(self.foreach_clause(clause)?),
                grammar::RULE_ORDER_BY_SKIP_LIMIT_CLAUSE => Clause::OrderByAndPage(self.order_by_and_page(clause)?),
                grammar::RULE_RETURN_CLAUSE => {
                    returned = true;
                    Clause::Return(self.return_clause(clause)?)
                }
                // §14.10 GQ27: FINISH ends the query, as RETURN does.
                grammar::RULE_FINISH_CLAUSE => {
                    returned = true;
                    Clause::Finish
                }
                grammar::RULE_OPTIONAL_BLOCK_CLAUSE => self.optional_block(clause)?,
                grammar::RULE_SUBQUERY_CLAUSE => Clause::Call(self.call_clause(clause)?),
                _ => return Err(self.clause_bucket(clause)),
            });
        }
        let has_mutation = lowered.iter().any(|clause| {
            matches!(
                clause,
                Clause::Create(_) | Clause::Merge(_) | Clause::Set(_) | Clause::Remove(_) | Clause::Delete(_) | Clause::Foreach(_)
            )
        });
        if !returned && !self.open_query && !has_mutation {
            // Read queries must end in RETURN; a mutation-only query is a
            // complete executable query without a result clause.
            return self.refuse();
        }
        Ok(Query {
            clauses: lowered,
            unions: Vec::new(),
        })
    }

    /// The bucket a clause this adapter does not lower belongs in. A clause
    /// that names no bucket of its own stays in the one being read.
    fn clause_bucket(&self, clause: Cst<'_>) -> QueryFailureReason {
        match clause.rule() {
            grammar::RULE_MATCH_CLAUSE => QueryFailureReason::Pattern,
            grammar::RULE_WITH_CLAUSE | grammar::RULE_RETURN_CLAUSE | grammar::RULE_LET_CLAUSE => QueryFailureReason::Return,
            grammar::RULE_FILTER_CLAUSE => QueryFailureReason::Where,
            grammar::RULE_UNWIND_CLAUSE => QueryFailureReason::Unwind,
            grammar::RULE_ORDER_BY_SKIP_LIMIT_CLAUSE => QueryFailureReason::OrderWindow,
            grammar::RULE_CREATE_CLAUSE
            | grammar::RULE_INSERT_CLAUSE
            | grammar::RULE_SET_CLAUSE
            | grammar::RULE_REMOVE_CLAUSE
            | grammar::RULE_DELETE_CLAUSE
            | grammar::RULE_MERGE_CLAUSE
            | grammar::RULE_FOREACH_CLAUSE => QueryFailureReason::Mutation,
            grammar::RULE_CALL_CLAUSE | grammar::RULE_SUBQUERY_CLAUSE => QueryFailureReason::Procedure,
            _ => self.context,
        }
    }

    fn create_clause(&mut self, node: Cst<'_>) -> Lowered<CreateClause> {
        self.context = QueryFailureReason::Mutation;
        let patterns = node
            .required(grammar::RULE_PATTERN_LIST)?
            .rules(grammar::RULE_PATTERN)
            .map(|pattern| self.pattern(pattern))
            .collect::<Lowered<Vec<_>>>()?;
        Ok(CreateClause { patterns })
    }

    fn merge_clause(&mut self, node: Cst<'_>) -> Lowered<MergeClause> {
        self.context = QueryFailureReason::Mutation;
        let pattern = self.pattern(node.required(grammar::RULE_PATTERN)?)?;
        let mut on_create = Vec::new();
        let mut on_match = Vec::new();
        for action in node.rules(grammar::RULE_MERGE_ACTION) {
            let items = self.set_clause(action.required(grammar::RULE_SET_CLAUSE)?)?.items;
            if action.has(grammar::CREATE) {
                on_create.extend(items);
            } else if action.has(grammar::MATCH) {
                on_match.extend(items);
            } else {
                return self.refuse();
            }
        }
        Ok(MergeClause {
            pattern,
            on_create,
            on_match,
        })
    }

    fn set_clause(&mut self, node: Cst<'_>) -> Lowered<SetClause> {
        self.context = QueryFailureReason::Mutation;
        let items = node
            .rules(grammar::RULE_SET_ITEM)
            .map(|item| self.set_item(item))
            .collect::<Lowered<Vec<_>>>()?;
        Ok(SetClause { items })
    }

    fn set_item(&mut self, node: Cst<'_>) -> Lowered<SetItem> {
        if let Some(labels) = node
            .first(grammar::RULE_NODE_LABELS)
            .or_else(|| node.first(grammar::RULE_NODE_LABELS_IS))
        {
            let variable = node.required(grammar::RULE_VARIABLE)?;
            return Ok(SetItem::Labels {
                target: Expression::Identifier(variable_name(variable)),
                labels: self.static_labels(labels)?,
            });
        }

        let target = match node.first(grammar::RULE_EXPRESSION2) {
            Some(expression) => self.expression2(expression)?,
            None => Expression::Identifier(variable_name(node.required(grammar::RULE_VARIABLE)?)),
        };
        let operator = if node.has(grammar::PLUSEQUAL) {
            PropertiesUpdate::Merge
        } else if node.has(grammar::EQ) {
            PropertiesUpdate::Replace
        } else {
            return self.refuse();
        };
        let value = self.expression(node.required(grammar::RULE_EXPRESSION)?)?;
        match target {
            Expression::Property { target, key } if operator == PropertiesUpdate::Replace => Ok(SetItem::Property {
                target: *target,
                property: key,
                value,
            }),
            Expression::Property { .. } => self.refuse(),
            target => Ok(SetItem::Properties { target, operator, value }),
        }
    }

    fn remove_clause(&mut self, node: Cst<'_>) -> Lowered<RemoveClause> {
        self.context = QueryFailureReason::Mutation;
        let items = node
            .rules(grammar::RULE_REMOVE_ITEM)
            .map(|item| self.remove_item(item))
            .collect::<Lowered<Vec<_>>>()?;
        Ok(RemoveClause { items })
    }

    fn remove_item(&mut self, node: Cst<'_>) -> Lowered<RemoveItem> {
        if let Some(labels) = node
            .first(grammar::RULE_NODE_LABELS)
            .or_else(|| node.first(grammar::RULE_NODE_LABELS_IS))
        {
            let variable = node.required(grammar::RULE_VARIABLE)?;
            return Ok(RemoveItem::Labels {
                target: Expression::Identifier(variable_name(variable)),
                labels: self.static_labels(labels)?,
            });
        }
        match self.expression2(node.required(grammar::RULE_EXPRESSION2)?)? {
            Expression::Property { target, key } => Ok(RemoveItem::Property {
                target: *target,
                property: key,
            }),
            _ => self.refuse(),
        }
    }

    fn static_labels(&mut self, node: Cst<'_>) -> Lowered<Vec<Identifier>> {
        if node.first(grammar::RULE_DYNAMIC_EXPRESSION).is_some() || node.first(grammar::RULE_DYNAMIC_LABEL_TYPE).is_some() {
            return self.refuse();
        }
        let mut labels = Vec::new();
        if node.rule() == grammar::RULE_NODE_LABELS_IS {
            if let Some(name) = node.first(grammar::RULE_SYMBOLIC_NAME_STRING) {
                labels.push(symbolic_name(name));
            }
        }
        for label in node.rules(grammar::RULE_LABEL_TYPE) {
            labels.push(symbolic_name(label.required(grammar::RULE_SYMBOLIC_NAME_STRING)?));
        }
        if labels.is_empty() {
            return self.refuse();
        }
        Ok(labels)
    }

    fn delete_clause(&mut self, node: Cst<'_>) -> Lowered<DeleteClause> {
        self.context = QueryFailureReason::Mutation;
        let expressions = node
            .rules(grammar::RULE_EXPRESSION)
            .map(|expression| self.expression(expression))
            .collect::<Lowered<Vec<_>>>()?;
        Ok(DeleteClause {
            expressions,
            detach: node.has(grammar::DETACH),
        })
    }

    fn foreach_clause(&mut self, node: Cst<'_>) -> Lowered<ForeachClause> {
        self.context = QueryFailureReason::Mutation;
        let variable = variable_name(node.required(grammar::RULE_VARIABLE)?);
        let expression = self.expression(node.required(grammar::RULE_EXPRESSION)?)?;
        let clauses = node
            .rules(grammar::RULE_CLAUSE)
            .map(|clause| self.mutation_clause(clause))
            .collect::<Lowered<Vec<_>>>()?;
        Ok(ForeachClause {
            variable,
            expression,
            clauses,
        })
    }

    fn mutation_clause(&mut self, node: Cst<'_>) -> Lowered<MutationClause> {
        let clause = node.only_rule()?;
        match clause.rule() {
            grammar::RULE_CREATE_CLAUSE => Ok(MutationClause::Create(self.create_clause(clause)?)),
            grammar::RULE_MERGE_CLAUSE => Ok(MutationClause::Merge(Box::new(self.merge_clause(clause)?))),
            grammar::RULE_SET_CLAUSE => Ok(MutationClause::Set(self.set_clause(clause)?)),
            grammar::RULE_REMOVE_CLAUSE => Ok(MutationClause::Remove(self.remove_clause(clause)?)),
            grammar::RULE_DELETE_CLAUSE => Ok(MutationClause::Delete(self.delete_clause(clause)?)),
            grammar::RULE_FOREACH_CLAUSE => Ok(MutationClause::Foreach(self.foreach_clause(clause)?)),
            _ => self.refuse(),
        }
    }

    /// `[OPTIONAL] CALL [(a, b)] { query }` (§15.1, §15.2): an inline
    /// procedure call. `CALL (*)` imports every name, as no scope clause
    /// does; `IN TRANSACTIONS` is not read here.
    fn call_clause(&mut self, node: Cst<'_>) -> Lowered<CallClause> {
        self.context = QueryFailureReason::Procedure;
        if node.first(grammar::RULE_SUBQUERY_IN_TRANSACTIONS_PARAMETERS).is_some() {
            return self.refuse();
        }
        let scope = match node.first(grammar::RULE_SUBQUERY_SCOPE) {
            Some(scope) if scope.has(grammar::TIMES) => None,
            Some(scope) => Some(scope.rules(grammar::RULE_VARIABLE).map(variable_name).collect()),
            None => None,
        };
        let open_query = std::mem::replace(&mut self.open_query, false);
        let query = self.query_with_local_definitions(node.required(grammar::RULE_QUERY_WITH_LOCAL_DEFINITIONS)?);
        self.open_query = open_query;
        self.context = QueryFailureReason::Procedure;
        Ok(CallClause {
            outputs_to_record: false,
            optional: node.has(grammar::OPTIONAL),
            scope,
            query: Box::new(query?),
        })
    }

    /// `OPTIONAL { MATCH ... }` (§14.4, GQ21): its match statements, which
    /// may be blocks themselves.
    fn optional_block(&mut self, node: Cst<'_>) -> Lowered<Clause> {
        self.context = QueryFailureReason::Pattern;
        let statements = node
            .rules(grammar::RULE_OPTIONAL_BLOCK_STATEMENT)
            .map(|statement| {
                let statement = statement.only_rule()?;
                match statement.rule() {
                    grammar::RULE_MATCH_CLAUSE => Ok(Clause::Match(self.match_clause(statement)?)),
                    _ => self.optional_block(statement),
                }
            })
            .collect::<Lowered<Vec<_>>>()?;
        Ok(Clause::OptionalBlock(statements))
    }

    fn match_clause(&mut self, node: Cst<'_>) -> Lowered<MatchClause> {
        self.context = QueryFailureReason::Pattern;
        if node.first(grammar::RULE_HINT).is_some() || node.first(grammar::RULE_SEARCH_CLAUSE).is_some() {
            return self.refuse();
        }
        let mode = node.first(grammar::RULE_MATCH_MODE).map(|mode| {
            if mode.has(grammar::REPEATABLE) {
                MatchMode::RepeatableElements
            } else {
                MatchMode::DifferentRelationships
            }
        });
        let patterns = node
            .required(grammar::RULE_PATTERN_LIST)?
            .rules(grammar::RULE_PATTERN)
            .map(|pattern| self.pattern(pattern))
            .collect::<Lowered<Vec<_>>>()?;
        let predicate = match node.first(grammar::RULE_WHERE_CLAUSE) {
            Some(clause) => {
                self.context = QueryFailureReason::Where;
                Some(self.expression(clause.required(grammar::RULE_EXPRESSION)?)?)
            }
            None => None,
        };
        // §16.4 G006, G007: the prefix after KEEP.
        let keep = node
            .first(grammar::RULE_PATH_PATTERN_PREFIX)
            .map(|prefix| self.path_prefix(prefix))
            .transpose()?;
        Ok(MatchClause {
            optional: node.has(grammar::OPTIONAL),
            mode,
            patterns,
            predicate,
            keep,
            // §16.3 GQ19: the clause's own variables are its yield items.
            yield_items: node
                .has(grammar::YIELD)
                .then(|| node.rules(grammar::RULE_VARIABLE).map(variable_name).collect()),
        })
    }

    fn pattern(&mut self, node: Cst<'_>) -> Lowered<PathPattern> {
        self.context = QueryFailureReason::Pattern;
        let factor = self.path_factor(node).map_err(|reason| match reason {
            QueryFailureReason::UntypedStatement => QueryFailureReason::Pattern,
            reason => reason,
        })?;
        let mut path = match self.executable_pattern(node) {
            Ok(path) => path,
            Err(QueryFailureReason::ParseError) => return Err(QueryFailureReason::ParseError),
            Err(_) => {
                let prefix = node.first(grammar::RULE_PATH_PATTERN_PREFIX);
                let written =
                    prefix.is_some_and(|prefix| prefix.has(grammar::ALL) || prefix.has(grammar::ANY) || prefix.has(grammar::SHORTEST));
                let prefix = prefix.map(|prefix| self.path_prefix(prefix)).transpose()?;
                PathPattern {
                    factor: None,
                    binding: node.first(grammar::RULE_VARIABLE).map(variable_name),
                    subpaths: Vec::new(),
                    search: prefix.as_ref().and_then(|prefix| prefix.search.clone()),
                    search_prefix_written: written,
                    elements: Vec::new(),
                    modes: prefix.and_then(|prefix| prefix.mode).into_iter().collect(),
                    predicate: None,
                    alternatives: None,
                }
            }
        };
        path.factor = Some(factor);
        Ok(path)
    }

    fn path_factor(&mut self, node: Cst<'_>) -> Lowered<hydradb_cypher_ast::PathFactor> {
        use hydradb_cypher_ast::PathFactor;
        let anonymous = node.required(grammar::RULE_ANONYMOUS_PATTERN)?;
        let terms = anonymous.rules(grammar::RULE_PATTERN_ELEMENT).collect::<Vec<_>>();
        let factors = terms.into_iter().map(|term| self.term_factor(term)).collect::<Lowered<Vec<_>>>()?;
        match factors.len() {
            0 => self.refuse(),
            1 => Ok(factors.into_iter().next().expect("one term")),
            _ => Ok(PathFactor::Alternatives(factors)),
        }
    }

    fn term_factor(&mut self, element: Cst<'_>) -> Lowered<hydradb_cypher_ast::PathFactor> {
        use hydradb_cypher_ast::PathFactor;
        let mut factors: Vec<PathFactor> = Vec::new();
        for child in element.children() {
            if let Child::Token(grammar::QUESTION, _) = child {
                let primary = factors.pop().ok_or(QueryFailureReason::ParseError)?;
                let primary = if matches!(primary, PathFactor::Edge(_)) {
                    PathFactor::concatenation(vec![primary])
                } else {
                    primary
                };
                factors.push(PathFactor::Questioned(Box::new(primary)));
                continue;
            }
            let Child::Rule(child) = child else {
                return self.refuse();
            };
            let factor = match child.rule() {
                grammar::RULE_NODE_PATTERN => PathFactor::Node(child.first(grammar::RULE_VARIABLE).map(variable_name)),
                grammar::RULE_RELATIONSHIP_PATTERN | grammar::RULE_ABBREVIATED_RELATIONSHIP_PATTERN => {
                    PathFactor::Edge(child.first(grammar::RULE_VARIABLE).map(variable_name))
                }
                grammar::RULE_TILDE_RELATIONSHIP_PATTERN => PathFactor::Edge(
                    child
                        .first(grammar::RULE_TILDE_EDGE_FILLER)
                        .and_then(|filler| filler.first(grammar::RULE_VARIABLE))
                        .map(variable_name),
                ),
                grammar::RULE_SIMPLIFIED_PATH_PATTERN => {
                    PathFactor::Simplified(self.simplified_algebra(child.required(grammar::RULE_SIMPLIFIED_CONTENTS)?)?)
                }
                grammar::RULE_PARENTHESIZED_PATH => {
                    let path = child.required(grammar::RULE_PATTERN)?;
                    let prefix = path.first(grammar::RULE_PATH_PATTERN_PREFIX);
                    let factor = PathFactor::Parenthesized {
                        binding: path.first(grammar::RULE_VARIABLE).map(variable_name),
                        restrictive: prefix
                            .and_then(|prefix| prefix.first(grammar::RULE_PATH_MODE))
                            .is_some_and(|mode| !mode.has(grammar::WALK)),
                        search_prefix_written: prefix
                            .is_some_and(|prefix| prefix.has(grammar::ALL) || prefix.has(grammar::ANY) || prefix.has(grammar::SHORTEST)),
                        inner: Box::new(self.path_factor(path)?),
                    };
                    match child.first(grammar::RULE_QUANTIFIER) {
                        Some(quantifier) => factor.quantified(self.quantifier(quantifier)?),
                        None => factor,
                    }
                }
                grammar::RULE_QUANTIFIER => {
                    let primary = factors.pop().ok_or(QueryFailureReason::ParseError)?;
                    factors.push(primary.quantified(self.quantifier(child)?));
                    continue;
                }
                _ => return self.refuse(),
            };
            factors.push(factor);
        }
        Ok(PathFactor::concatenation(factors))
    }

    fn executable_pattern(&mut self, node: Cst<'_>) -> Lowered<PathPattern> {
        // §16.4 G004: `p = ...`.
        let binding = node.first(grammar::RULE_VARIABLE).map(variable_name);
        let mut modes = Vec::new();
        let mut search = None;
        let mut search_prefix_written = false;
        if let Some(prefix) = node.first(grammar::RULE_PATH_PATTERN_PREFIX) {
            search_prefix_written = prefix.has(grammar::ALL) || prefix.has(grammar::ANY) || prefix.has(grammar::SHORTEST);
            let prefix = self.path_prefix(prefix)?;
            modes.extend(prefix.mode);
            search = prefix.search;
        }
        let anonymous = node.required(grammar::RULE_ANONYMOUS_PATTERN)?;
        let terms: Vec<_> = anonymous.rules(grammar::RULE_PATTERN_ELEMENT).collect();
        let mut path = match terms.as_slice() {
            // `shortestPath(...)` and `allShortestPaths(...)`.
            [] => return self.refuse(),
            [term] => self.path_term(*term)?,
            terms => alternative_paths(
                anonymous.has(grammar::MULTISET_ALTERNATION),
                terms.iter().map(|term| self.path_term(*term)).collect::<Lowered<Vec<_>>>()?,
            )?,
        };
        path.search = search;
        path.search_prefix_written |= search_prefix_written;
        modes.append(&mut path.modes);
        path.modes = modes;
        path.binding = binding;
        Ok(path)
    }

    /// The same composition adapter joins ordinary, parenthesized and
    /// simplified factors. Scoped boundaries are retained or refused, never erased.
    fn path_term(&mut self, element: Cst<'_>) -> Lowered<PathPattern> {
        let mut factors: Vec<(PathPattern, bool)> = Vec::new();
        for child in element.children() {
            if let Child::Token(grammar::QUESTION, _) = child {
                let (path, questioned) = factors.last_mut().ok_or(self.context)?;
                if *questioned
                    || path.alternatives.is_some()
                    || path.binding.is_some()
                    || !path.subpaths.is_empty()
                    || !path.modes.is_empty()
                    || path.search_prefix_written
                    || path
                        .elements
                        .iter()
                        .any(|element| matches!(element, PatternElement::Relationship(edge) if edge.length.is_some()))
                {
                    return self.refuse();
                }
                *questioned = true;
                continue;
            }
            let Child::Rule(part) = child else { return self.refuse() };
            let path = match part.rule() {
                grammar::RULE_NODE_PATTERN => path_elements(vec![PatternElement::Node(self.node_pattern(part)?)]),
                grammar::RULE_RELATIONSHIP_PATTERN | grammar::RULE_ABBREVIATED_RELATIONSHIP_PATTERN => {
                    path_elements(vec![PatternElement::Relationship(self.relationship_pattern(part)?)])
                }
                grammar::RULE_TILDE_RELATIONSHIP_PATTERN => {
                    path_elements(vec![PatternElement::Relationship(self.tilde_relationship_pattern(part)?)])
                }
                grammar::RULE_SIMPLIFIED_PATH_PATTERN => self.simplified_path(part)?,
                grammar::RULE_QUANTIFIER => {
                    let (path, questioned) = factors.pop().ok_or(self.context)?;
                    if questioned {
                        return self.refuse();
                    }
                    factors.push((quantify_adapter(path, self.quantifier(part)?)?, false));
                    continue;
                }
                grammar::RULE_PARENTHESIZED_PATH if part.first(grammar::RULE_QUANTIFIER).is_some() => {
                    path_elements(vec![PatternElement::Relationship(self.quantified_path(part)?)])
                }
                grammar::RULE_PARENTHESIZED_PATH => self.parenthesized_path(part)?,
                _ => return self.refuse(),
            };
            factors.push((path, false));
        }
        if factors.iter().any(|(_, questioned)| *questioned) {
            return questioned_union(factors);
        }
        let mut factors = factors.into_iter().map(|(path, _)| path);
        let mut path = factors.next().ok_or(self.context)?;
        for factor in factors {
            path = concatenate_paths(path, factor)?;
        }
        finish_path(&mut path)?;
        Ok(path)
    }

    fn parenthesized_path(&mut self, node: Cst<'_>) -> Lowered<PathPattern> {
        let mut inner = self.pattern(node.required(grammar::RULE_PATTERN)?)?;
        if inner.search.is_some() {
            return self.refuse();
        }
        if let Some(predicate) = node.first(grammar::RULE_EXPRESSION) {
            let context = self.context;
            self.context = QueryFailureReason::Where;
            let predicate = self.expression(predicate)?;
            self.context = context;
            inner.predicate = Some(match inner.predicate.take() {
                Some(held) => Expression::Binary {
                    left: Box::new(held),
                    operator: BinaryOperator::And,
                    right: Box::new(predicate),
                },
                None => predicate,
            });
        }
        inner.subpaths.extend(inner.binding.take());
        Ok(inner)
    }

    /// A SQL <interval qualifier> (GL12, ISO/IEC 9075-2 §10.1). A lone
    /// SECOND's precisions are its leading and fractional ones; an end
    /// SECOND's one precision is its fractional one.
    fn interval_qualifier(&self, node: Cst<'_>) -> Lowered<IntervalQualifier> {
        let field = |node: Cst<'_>| -> Lowered<(IntervalField, Vec<u32>)> {
            let field = if node.has(grammar::YEAR) {
                IntervalField::Year
            } else if node.has(grammar::MONTH) {
                IntervalField::Month
            } else if node.has(grammar::DAY) {
                IntervalField::Day
            } else if node.has(grammar::HOUR) {
                IntervalField::Hour
            } else if node.has(grammar::MINUTE) {
                IntervalField::Minute
            } else {
                IntervalField::Second
            };
            let precisions = node
                .tokens()
                .filter(|(kind, _)| *kind == grammar::UNSIGNED_DECIMAL_INTEGER)
                .map(|(_, text)| {
                    digits(text)
                        .and_then(|value| u32::try_from(value).ok())
                        .ok_or(QueryFailureReason::ParseError)
                })
                .collect::<Lowered<Vec<_>>>()?;
            Ok((field, precisions))
        };
        let mut fields = node.rules(grammar::RULE_INTERVAL_FIELD);
        let (start, start_precisions) = field(fields.next().ok_or(QueryFailureReason::ParseError)?)?;
        let end = fields.next().map(field).transpose()?;
        let (leading_precision, mut fractional_precision) = match (start, start_precisions.as_slice()) {
            (_, []) => (None, None),
            (IntervalField::Second, [leading, fractional]) if end.is_none() => (Some(*leading), Some(*fractional)),
            (_, [leading]) => (Some(*leading), None),
            _ => return Err(QueryFailureReason::ParseError),
        };
        if let Some((end, precisions)) = &end {
            match (end, precisions.as_slice()) {
                (_, []) => {}
                (IntervalField::Second, [fractional]) => fractional_precision = Some(*fractional),
                _ => return Err(QueryFailureReason::ParseError),
            }
        }
        Ok(IntervalQualifier {
            start,
            end: end.map(|(end, _)| end),
            leading_precision,
            fractional_precision,
        })
    }

    /// A <path pattern prefix> (§16.6), normalized by its Syntax Rule 2: the
    /// mode, if one is written, and the selective search, if any. `ALL`
    /// alone (G015) selects nothing; `PATH` and `PATHS` say nothing.
    fn path_prefix(&mut self, prefix: Cst<'_>) -> Lowered<PathPrefix> {
        let mode = match prefix.first(grammar::RULE_PATH_MODE) {
            None => None,
            Some(mode) if mode.has(grammar::WALK) => Some(PathMode::Walk),
            Some(mode) if mode.has(grammar::TRAIL) => Some(PathMode::Trail),
            Some(mode) if mode.has(grammar::ACYCLIC) => Some(PathMode::Acyclic),
            Some(_) => Some(PathMode::Simple),
        };
        let count = match prefix.first(grammar::RULE_NON_NEGATIVE_INTEGER_SPECIFICATION) {
            None => Expression::Literal(Literal::Integer(1)),
            Some(count) => match count.first(grammar::RULE_PARAMETER) {
                Some(parameter) => Expression::Parameter(parameter_name(parameter)?),
                None => Expression::Literal(Literal::Integer(digits(&count.text()).ok_or(QueryFailureReason::ParseError)?)),
            },
        };
        let kind = if prefix.has(grammar::SHORTEST) {
            if prefix.has(grammar::ALL) || prefix.first(grammar::RULE_GROUP_TOKEN).is_some() {
                Some(PathSearchKind::ShortestGroups)
            } else {
                Some(PathSearchKind::Shortest)
            }
        } else if prefix.has(grammar::ANY) {
            Some(PathSearchKind::Any)
        } else {
            None
        };
        Ok(PathPrefix {
            mode,
            search: kind.map(|kind| PathSearch { kind, count }),
        })
    }

    /// A <graph pattern quantifier> (§16.11), normalized: `*` is `{0,}`, `+`
    /// is `{1,}`, `{n}` is `{n,n}` and an absent lower bound is 0. Its bounds
    /// are checked in planning.
    fn quantifier(&self, node: Cst<'_>) -> Lowered<PathLength> {
        let bound = |kind, text: &str| {
            unsigned_integer(kind, text)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or(QueryFailureReason::ParseError)
        };
        let unbounded = |minimum| PathLength {
            minimum: Some(minimum),
            maximum: None,
            quantifier: true,
        };
        if node.has(grammar::PLUS) {
            return Ok(unbounded(1));
        }
        if node.has(grammar::TIMES) {
            return Ok(unbounded(0));
        }
        let mut minimum = None;
        let mut maximum = None;
        let mut comma = false;
        for (kind, text) in node.tokens() {
            match kind {
                grammar::COMMA => comma = true,
                grammar::UNSIGNED_DECIMAL_INTEGER
                | grammar::UNSIGNED_HEX_INTEGER
                | grammar::UNSIGNED_OCTAL_INTEGER
                | grammar::UNSIGNED_BINARY_INTEGER => {
                    let value = bound(kind, text)?;
                    if comma {
                        maximum = Some(value);
                    } else {
                        minimum = Some(value);
                    }
                }
                _ => {}
            }
        }
        if !comma {
            maximum = minimum;
        }
        Ok(PathLength {
            minimum: Some(minimum.unwrap_or(0)),
            maximum,
            quantifier: true,
        })
    }

    fn node_pattern(&mut self, node: Cst<'_>) -> Lowered<NodePattern> {
        let variable = node.first(grammar::RULE_VARIABLE).map(variable_name);
        let (labels, label_expression) = match node.first(grammar::RULE_LABEL_EXPRESSION) {
            Some(expression) => self.node_labels(expression)?,
            None => (Vec::new(), None),
        };
        let properties = node
            .first(grammar::RULE_PROPERTIES)
            .map(|properties| self.properties(properties))
            .transpose()?;
        let predicate = self.element_predicate(node)?;
        Ok(NodePattern {
            variable,
            labels,
            label_expression,
            properties,
            predicate,
        })
    }

    fn relationship_pattern(&mut self, node: Cst<'_>) -> Lowered<RelationshipPattern> {
        let variable = node.first(grammar::RULE_VARIABLE).map(variable_name);
        let (types, label_expression) = match node.first(grammar::RULE_LABEL_EXPRESSION) {
            Some(expression) => self.relationship_types(expression)?,
            None => (Vec::new(), None),
        };
        let length = node
            .first(grammar::RULE_PATH_LENGTH)
            .map(|length| self.path_length(length))
            .transpose()?;
        let properties = node
            .first(grammar::RULE_PROPERTIES)
            .map(|properties| self.properties(properties))
            .transpose()?;
        let predicate = self.element_predicate(node)?;
        // The closing side decides the direction: `-[..]->` is outgoing,
        // `<-[..]-` incoming, `-[..]-` undirected, and `<-[..]->` the
        // bidirectional spelling, which means the same as undirected.
        let direction = match (
            node.first(grammar::RULE_LEFT_ARROW).is_some(),
            node.first(grammar::RULE_RIGHT_ARROW).is_some(),
        ) {
            (false, true) => Direction::Outgoing,
            (true, false) => Direction::Incoming,
            (false, false) => Direction::Undirected,
            (true, true) => Direction::Bidirectional,
        };
        Ok(RelationshipPattern {
            variable,
            types,
            label_expression,
            direction,
            length,
            properties,
            predicate,
            iteration_nodes: None,
        })
    }

    /// `((a)-[e:R]->(b)){m,n}` (§16.7, G035) as the hop it repeats: `e` a
    /// quantified edge, and `a` and `b` the group variables of each
    /// iteration's first and last node. One edge between nodes that carry no
    /// label, property or WHERE of their own: an intermediate node is both,
    /// so its conditions would hold at every step, which the expansion does
    /// not check.
    fn quantified_path(&mut self, node: Cst<'_>) -> Lowered<RelationshipPattern> {
        let Some(quantifier) = node.first(grammar::RULE_QUANTIFIER) else {
            // A parenthesized path inside a longer one (G038 elsewhere).
            return self.refuse();
        };
        // Every part of the inner path is named, so one this rewrite does not
        // carry into each iteration is refused rather than dropped.
        let mut primary = self.pattern(node.required(grammar::RULE_PATTERN)?)?;
        if let Some(predicate) = self.element_predicate(node)? {
            primary.predicate = Some(match primary.predicate {
                Some(previous) => Expression::Binary {
                    left: Box::new(previous),
                    operator: BinaryOperator::And,
                    right: Box::new(predicate),
                },
                None => predicate,
            });
        }
        let PathPattern {
            factor: _,
            binding,
            search,
            search_prefix_written,
            elements,
            modes,
            predicate: _,
            subpaths,
            alternatives,
        } = primary.clone();
        if binding.is_some()
            || !subpaths.is_empty()
            || !modes.is_empty()
            || search_prefix_written
            || search.is_some()
            || alternatives.is_some()
        {
            return self.refuse();
        }
        let [PatternElement::Node(start), PatternElement::Relationship(edge), PatternElement::Node(end)] = elements.as_slice() else {
            return self.refuse();
        };
        Ok(RelationshipPattern {
            length: Some(self.quantifier(quantifier)?),
            properties: None,
            predicate: None,
            label_expression: None,
            iteration_nodes: Some(IterationNodes {
                primary: Box::new(primary),
                start: start.variable.clone(),
                end: end.variable.clone(),
                nested: edge.length.is_some(),
            }),
            ..edge.clone()
        })
    }

    /// The `WHERE` a node or relationship pattern may carry before it closes,
    /// as in `(n:Item WHERE n.score > 2)`. Restores the clause context
    /// afterwards: the predicate sits inside a pattern, and a failure after it
    /// is still the pattern's.
    fn element_predicate(&mut self, node: Cst<'_>) -> Lowered<Option<Expression>> {
        let Some(predicate) = node.first(grammar::RULE_EXPRESSION) else {
            return Ok(None);
        };
        let context = self.context;
        self.context = QueryFailureReason::Where;
        let predicate = self.expression(predicate)?;
        self.context = context;
        Ok(Some(predicate))
    }

    /// A node's labels: a plain conjunction such as `(:A:B)`, `(:A&B)` or
    /// `(IS A&B)` stays a label list; anything else is also carried as a
    /// label expression, with the names it requires in the list.
    fn node_labels(&self, node: Cst<'_>) -> Lowered<(Vec<Identifier>, Option<LabelExpression>)> {
        let expression = self.label_expression(node.required(grammar::RULE_LABEL_EXPRESSION4)?)?;
        Ok(match expression {
            LabelExpression::Label(name) => (vec![name], None),
            LabelExpression::And(terms) if terms.iter().all(|term| matches!(term, LabelExpression::Label(_))) => (
                terms
                    .into_iter()
                    .map(|term| match term {
                        LabelExpression::Label(name) => name,
                        _ => unreachable!("checked above"),
                    })
                    .collect(),
                None,
            ),
            LabelExpression::And(terms) => {
                let required = terms
                    .iter()
                    .filter_map(|term| match term {
                        LabelExpression::Label(name) => Some(name.clone()),
                        _ => None,
                    })
                    .collect();
                (required, Some(LabelExpression::And(terms)))
            }
            other => (Vec::new(), Some(other)),
        })
    }

    /// A relationship's types: a plain disjunction such as `[:A]` or `[:A|B]`
    /// stays a type list; anything else is a label expression.
    fn relationship_types(&self, node: Cst<'_>) -> Lowered<(Vec<Identifier>, Option<LabelExpression>)> {
        Ok(type_list(self.label_expression(node.required(grammar::RULE_LABEL_EXPRESSION4)?)?))
    }

    /// A `type`, `typePart (BAR typePart)*`, as a <value type> (§18.9):
    /// several parts are a closed dynamic union.
    fn value_type(&self, node: Cst<'_>) -> Lowered<ValueType> {
        let mut parts = node
            .rules(grammar::RULE_TYPE_PART)
            .map(|part| self.type_part(part))
            .collect::<Lowered<Vec<_>>>()?;
        Ok(if parts.len() == 1 {
            parts.remove(0)
        } else {
            ValueType {
                kind: ValueTypeKind::Union(parts),
                not_null: false,
            }
        })
    }

    /// `typeName typeNullability? typeListSuffix*`: each suffix a list of
    /// what precedes it, `INTEGER LIST` being `LIST<INTEGER>`.
    fn type_part(&self, node: Cst<'_>) -> Lowered<ValueType> {
        let mut value_type = ValueType {
            kind: self.type_name(node.required(grammar::RULE_TYPE_NAME)?)?,
            not_null: node.first(grammar::RULE_TYPE_NULLABILITY).is_some(),
        };
        for suffix in node.rules(grammar::RULE_TYPE_LIST_SUFFIX) {
            value_type = ValueType {
                kind: ValueTypeKind::List {
                    element: Some(Box::new(value_type)),
                    max_length: suffix.first(grammar::RULE_LIST_MAXIMUM_LENGTH).map(type_length).transpose()?,
                },
                not_null: suffix.first(grammar::RULE_TYPE_NULLABILITY).is_some(),
            };
        }
        Ok(value_type)
    }

    /// One type name, read by its first word as the Cypher grammar's own
    /// rule is.
    fn type_name(&self, node: Cst<'_>) -> Lowered<ValueTypeKind> {
        let words: Vec<i32> = node.tokens().map(|(kind, _)| kind).collect();
        let lengths = node
            .tokens()
            .filter(|(kind, _)| *kind == grammar::UNSIGNED_DECIMAL_INTEGER)
            .map(|(_, text)| {
                digits(text)
                    .and_then(|value| u64::try_from(value).ok())
                    .ok_or(QueryFailureReason::ParseError)
            })
            .collect::<Lowered<Vec<u64>>>()?;
        let has = |word: i32| words.contains(&word);
        let integer = |signed: bool, size: IntegerSize| ValueTypeKind::Integer { signed, size };
        let precision = |size: IntegerSize| lengths.first().map_or(size, |precision| IntegerSize::Precision(*precision));
        let Some(&first) = words.first() else {
            return self.refuse();
        };
        Ok(match first {
            // A graph reference type, whichever word it starts with (`ANY
            // GRAPH`, `PROPERTY GRAPH`): first, so a type `PROPERTY` or `ANY`
            // begins is not read as a value type.
            _ if has(grammar::GRAPH) => ValueTypeKind::Graph,
            grammar::NOTHING => ValueTypeKind::Nothing,
            grammar::NULL => ValueTypeKind::Null,
            grammar::BOOL | grammar::BOOLEAN => ValueTypeKind::Boolean,
            grammar::STRING => ValueTypeKind::CharacterString {
                min_length: (lengths.len() == 2).then(|| lengths[0]),
                max_length: lengths.last().copied(),
            },
            grammar::VARCHAR => ValueTypeKind::CharacterString {
                min_length: None,
                max_length: lengths.first().copied(),
            },
            // §18.9 Syntax Rule 11: an omitted fixed length is 1.
            grammar::CHAR => {
                let fixed = lengths.first().copied().unwrap_or(1);
                ValueTypeKind::CharacterString {
                    min_length: Some(fixed),
                    max_length: Some(fixed),
                }
            }
            grammar::BYTES => ValueTypeKind::ByteString {
                min_length: (lengths.len() == 2).then(|| lengths[0]),
                max_length: lengths.last().copied(),
            },
            grammar::VARBINARY => ValueTypeKind::ByteString {
                min_length: None,
                max_length: lengths.first().copied(),
            },
            grammar::BINARY => {
                let fixed = lengths.first().copied().unwrap_or(1);
                ValueTypeKind::ByteString {
                    min_length: Some(fixed),
                    max_length: Some(fixed),
                }
            }
            grammar::INT8 => integer(true, IntegerSize::Bits(8)),
            grammar::INT16 => integer(true, IntegerSize::Bits(16)),
            grammar::INT32 => integer(true, IntegerSize::Bits(32)),
            grammar::INT64 => integer(true, IntegerSize::Bits(64)),
            grammar::INT128 => integer(true, IntegerSize::Bits(128)),
            grammar::INT256 => integer(true, IntegerSize::Bits(256)),
            grammar::SMALLINT => integer(true, IntegerSize::Small),
            grammar::BIGINT => integer(true, IntegerSize::Big),
            grammar::INT => integer(true, precision(IntegerSize::Regular)),
            grammar::UINT8 => integer(false, IntegerSize::Bits(8)),
            grammar::UINT16 => integer(false, IntegerSize::Bits(16)),
            grammar::UINT32 => integer(false, IntegerSize::Bits(32)),
            grammar::UINT64 => integer(false, IntegerSize::Bits(64)),
            grammar::UINT128 => integer(false, IntegerSize::Bits(128)),
            grammar::UINT256 => integer(false, IntegerSize::Bits(256)),
            grammar::USMALLINT => integer(false, IntegerSize::Small),
            grammar::UBIGINT => integer(false, IntegerSize::Big),
            grammar::UINT => integer(false, precision(IntegerSize::Regular)),
            grammar::SIGNED
            | grammar::UNSIGNED
            | grammar::INTEGER
            | grammar::INTEGER8
            | grammar::INTEGER16
            | grammar::INTEGER32
            | grammar::INTEGER64
            | grammar::INTEGER128
            | grammar::INTEGER256
            | grammar::SMALL
            | grammar::BIG => {
                let size = if has(grammar::INTEGER8) {
                    IntegerSize::Bits(8)
                } else if has(grammar::INTEGER16) {
                    IntegerSize::Bits(16)
                } else if has(grammar::INTEGER32) {
                    IntegerSize::Bits(32)
                } else if has(grammar::INTEGER64) {
                    IntegerSize::Bits(64)
                } else if has(grammar::INTEGER128) {
                    IntegerSize::Bits(128)
                } else if has(grammar::INTEGER256) {
                    IntegerSize::Bits(256)
                } else if has(grammar::SMALL) {
                    IntegerSize::Small
                } else if has(grammar::BIG) {
                    IntegerSize::Big
                } else {
                    precision(IntegerSize::Regular)
                };
                integer(!has(grammar::UNSIGNED), size)
            }
            grammar::DECIMAL | grammar::DEC => ValueTypeKind::Decimal {
                precision: lengths.first().copied(),
                scale: lengths.get(1).copied(),
            },
            grammar::FLOAT16 => ValueTypeKind::Float(FloatSize::Bits(16)),
            grammar::FLOAT32 => ValueTypeKind::Float(FloatSize::Bits(32)),
            grammar::FLOAT64 => ValueTypeKind::Float(FloatSize::Bits(64)),
            grammar::FLOAT128 => ValueTypeKind::Float(FloatSize::Bits(128)),
            grammar::FLOAT256 => ValueTypeKind::Float(FloatSize::Bits(256)),
            grammar::REAL => ValueTypeKind::Float(FloatSize::Real),
            grammar::DOUBLE => ValueTypeKind::Float(FloatSize::Double),
            grammar::FLOAT => ValueTypeKind::Float(match lengths.as_slice() {
                [] => FloatSize::Regular,
                [precision] => FloatSize::Precision {
                    precision: *precision,
                    scale: None,
                },
                [precision, scale, ..] => FloatSize::Precision {
                    precision: *precision,
                    scale: Some(*scale),
                },
            }),
            grammar::DATE => ValueTypeKind::Date,
            grammar::LOCAL if has(grammar::DATETIME) => ValueTypeKind::LocalDatetime,
            grammar::LOCAL => ValueTypeKind::LocalTime,
            grammar::ZONED if has(grammar::DATETIME) => ValueTypeKind::ZonedDatetime,
            grammar::ZONED => ValueTypeKind::ZonedTime,
            grammar::TIME if has(grammar::WITH) => ValueTypeKind::ZonedTime,
            grammar::TIME => ValueTypeKind::LocalTime,
            grammar::TIMESTAMP if has(grammar::WITH) => ValueTypeKind::ZonedDatetime,
            grammar::TIMESTAMP => ValueTypeKind::LocalDatetime,
            grammar::DURATION => ValueTypeKind::Duration {
                year_month: node
                    .first(grammar::RULE_TEMPORAL_DURATION_QUALIFIER)
                    .map(|qualifier| qualifier.has(grammar::YEAR)),
            },
            grammar::NODE | grammar::VERTEX => ValueTypeKind::Node,
            grammar::RELATIONSHIP | grammar::EDGE => ValueTypeKind::Edge,
            grammar::PATH | grammar::PATHS => ValueTypeKind::Path,
            grammar::LIST | grammar::ARRAY => ValueTypeKind::List {
                element: node
                    .first(grammar::RULE_TYPE)
                    .map(|element| self.value_type(element))
                    .transpose()?
                    .map(Box::new),
                max_length: node.first(grammar::RULE_LIST_MAXIMUM_LENGTH).map(type_length).transpose()?,
            },
            grammar::PROPERTY => ValueTypeKind::PropertyValue,
            // §18.9 GV46, GV47: a record type with its field types, or open.
            grammar::RECORD | grammar::LCURLY if has(grammar::LCURLY) => ValueTypeKind::Record {
                fields: Some(
                    node.rules(grammar::RULE_FIELD_TYPE)
                        .map(|field| {
                            Ok((
                                symbolic_name(field.required(grammar::RULE_SYMBOLIC_NAME_STRING)?),
                                self.value_type(field.required(grammar::RULE_TYPE)?)?,
                            ))
                        })
                        .collect::<Lowered<Vec<_>>>()?,
                ),
            },
            grammar::RECORD => ValueTypeKind::Record { fields: None },
            grammar::ANY if has(grammar::RECORD) => ValueTypeKind::Record { fields: None },
            grammar::ANY if has(grammar::NODE) || has(grammar::VERTEX) => ValueTypeKind::Node,
            grammar::ANY if has(grammar::RELATIONSHIP) || has(grammar::EDGE) => ValueTypeKind::Edge,
            grammar::ANY if has(grammar::PROPERTY) => ValueTypeKind::PropertyValue,
            grammar::ANY if has(grammar::MAP) => ValueTypeKind::Cypher(Identifier::new("MAP")),
            grammar::ANY => match node.first(grammar::RULE_TYPE) {
                // `ANY<T>`: a union of the parts of T, one or more.
                Some(components) => match self.value_type(components)? {
                    ValueType {
                        kind: ValueTypeKind::Union(parts),
                        ..
                    } => ValueTypeKind::Union(parts),
                    one => ValueTypeKind::Union(vec![one]),
                },
                None => ValueTypeKind::Any,
            },
            // MAP, POINT, VECTOR, UUID: named by their first word.
            _ => ValueTypeKind::Cypher(Identifier::new(
                node.tokens()
                    .next()
                    .map_or(String::new(), |(_, word)| word.to_ascii_uppercase())
                    .as_str(),
            )),
        })
    }

    /// `~[ ]~`, `<~[ ]~`, `~[ ]~>` and their abbreviations `~`, `<~`, `~>`
    /// (§16.7, G043, G045).
    fn tilde_relationship_pattern(&mut self, node: Cst<'_>) -> Lowered<RelationshipPattern> {
        let direction = if node.has(grammar::LT) {
            Direction::LeftOrUndirected
        } else if node.has(grammar::GT) {
            Direction::UndirectedOrRight
        } else {
            Direction::UndirectedEdge
        };
        let Some(filler) = node.first(grammar::RULE_TILDE_EDGE_FILLER) else {
            return Ok(simplified_edge(direction, None));
        };
        let (types, label_expression) = match filler.first(grammar::RULE_LABEL_EXPRESSION) {
            Some(expression) => self.relationship_types(expression)?,
            None => (Vec::new(), None),
        };
        let properties = filler
            .first(grammar::RULE_PROPERTIES)
            .map(|properties| self.properties(properties))
            .transpose()?;
        Ok(RelationshipPattern {
            variable: filler.first(grammar::RULE_VARIABLE).map(variable_name),
            types,
            label_expression,
            direction,
            length: None,
            properties,
            predicate: self.element_predicate(filler)?,
            iteration_nodes: None,
        })
    }

    /// §16.12 SR6 adapts the retained algebra to ordinary path factors.
    /// Delimiters supply default directions; overrides supply their own.
    fn simplified_path(&mut self, node: Cst<'_>) -> Lowered<PathPattern> {
        let left = node.first(grammar::RULE_LEFT_ARROW).is_some() || node.has(grammar::LT);
        let right = node.first(grammar::RULE_RIGHT_ARROW).is_some() || node.has(grammar::GT);
        let direction = match (node.has(grammar::TILDE), left, right) {
            (false, false, true) => Direction::Outgoing,
            (false, true, false) => Direction::Incoming,
            (false, false, false) => Direction::Undirected,
            (false, true, true) => Direction::Bidirectional,
            (true, false, false) => Direction::UndirectedEdge,
            (true, true, false) => Direction::LeftOrUndirected,
            (true, false, true) => Direction::UndirectedOrRight,
            (true, true, true) => return self.refuse(),
        };
        simplified_adapter(
            &self.simplified_algebra(node.required(grammar::RULE_SIMPLIFIED_CONTENTS)?)?,
            direction,
        )
    }

    fn simplified_algebra(&self, node: Cst<'_>) -> Lowered<hydradb_cypher_ast::SimplifiedPath> {
        use hydradb_cypher_ast::SimplifiedPath as S;
        let children = |rule| {
            node.rules(rule)
                .map(|child| self.simplified_algebra(child))
                .collect::<Lowered<Vec<_>>>()
        };
        let single_or = |mut parts: Vec<S>, make: fn(Vec<S>) -> S| {
            if parts.len() == 1 {
                parts.remove(0)
            } else {
                make(parts)
            }
        };
        Ok(match node.rule() {
            grammar::RULE_SIMPLIFIED_CONTENTS => {
                let mut operands = children(grammar::RULE_SIMPLIFIED_TERM)?;
                if operands.len() == 1 {
                    operands.remove(0)
                } else {
                    S::Alternatives {
                        multiset: node.has(grammar::MULTISET_ALTERNATION),
                        operands,
                    }
                }
            }
            grammar::RULE_SIMPLIFIED_TERM => single_or(children(grammar::RULE_SIMPLIFIED_FACTOR_LOW)?, S::Concatenation),
            grammar::RULE_SIMPLIFIED_FACTOR_LOW => single_or(children(grammar::RULE_SIMPLIFIED_FACTOR_HIGH)?, S::Conjunction),
            grammar::RULE_SIMPLIFIED_FACTOR_HIGH => {
                let primary = Box::new(self.simplified_algebra(node.required(grammar::RULE_SIMPLIFIED_TERTIARY)?)?);
                if let Some(quantifier) = node.first(grammar::RULE_QUANTIFIER) {
                    S::Quantified {
                        primary,
                        length: self.quantifier(quantifier)?,
                    }
                } else if node.has(grammar::QUESTION) {
                    S::Questioned(primary)
                } else {
                    *primary
                }
            }
            grammar::RULE_SIMPLIFIED_TERTIARY => {
                let primary = Box::new(self.simplified_algebra(node.required(grammar::RULE_SIMPLIFIED_SECONDARY)?)?);
                match simplified_override(node) {
                    Some(direction) => S::DirectionOverride { direction, primary },
                    None => *primary,
                }
            }
            grammar::RULE_SIMPLIFIED_SECONDARY => {
                let primary = Box::new(self.simplified_algebra(node.required(grammar::RULE_SIMPLIFIED_PRIMARY)?)?);
                if node.has(grammar::EXCLAMATION_MARK) {
                    S::Negated(primary)
                } else {
                    *primary
                }
            }
            grammar::RULE_SIMPLIFIED_PRIMARY => match node.first(grammar::RULE_SYMBOLIC_NAME_STRING) {
                Some(name) => S::Label(symbolic_name(name)),
                None => self.simplified_algebra(node.required(grammar::RULE_SIMPLIFIED_CONTENTS)?)?,
            },
            _ => return self.refuse(),
        })
    }

    /// `labelExpression4: labelExpression3 (BAR COLON? labelExpression3)*`,
    /// a disjunction of conjunctions of possibly negated primaries.
    fn label_expression(&self, node: Cst<'_>) -> Lowered<LabelExpression> {
        let mut terms = node
            .rules(grammar::RULE_LABEL_EXPRESSION3)
            .map(|term| {
                let mut factors = term
                    .rules(grammar::RULE_LABEL_EXPRESSION2)
                    .map(|factor| self.label_factor(factor))
                    .collect::<Lowered<Vec<_>>>()?;
                Ok(if factors.len() == 1 {
                    factors.remove(0)
                } else {
                    LabelExpression::And(factors)
                })
            })
            .collect::<Lowered<Vec<_>>>()?;
        Ok(if terms.len() == 1 {
            terms.remove(0)
        } else {
            LabelExpression::Or(terms)
        })
    }

    /// `labelExpression2: EXCLAMATION_MARK* labelExpression1`.
    fn label_factor(&self, factor: Cst<'_>) -> Lowered<LabelExpression> {
        let primary = factor.required(grammar::RULE_LABEL_EXPRESSION1)?;
        let mut expression = if let Some(name) = primary.first(grammar::RULE_SYMBOLIC_NAME_STRING) {
            LabelExpression::Label(symbolic_name(name))
        } else if primary.has(grammar::PERCENT) {
            LabelExpression::Wildcard
        } else if let Some(inner) = primary.first(grammar::RULE_LABEL_EXPRESSION4) {
            self.label_expression(inner)?
        } else {
            // A dynamic label, `$(expression)`.
            return self.refuse();
        };
        for _ in factor.tokens().filter(|(kind, _)| *kind == grammar::EXCLAMATION_MARK) {
            expression = LabelExpression::Not(Box::new(expression));
        }
        Ok(expression)
    }

    fn path_length(&self, node: Cst<'_>) -> Lowered<PathLength> {
        let mut minimum = None;
        let mut maximum = None;
        let mut ranged = false;
        for (kind, text) in node.tokens() {
            match kind {
                grammar::DOTDOT => ranged = true,
                grammar::UNSIGNED_DECIMAL_INTEGER => {
                    let bound = digits(text).and_then(|value| u32::try_from(value).ok()).ok_or(self.context)?;
                    if ranged {
                        maximum = Some(bound);
                    } else {
                        minimum = Some(bound);
                    }
                }
                _ => {}
            }
        }
        if !ranged {
            // `*3` is exactly three hops; `*` is unbounded either way.
            maximum = minimum;
        }
        Ok(PathLength {
            minimum,
            maximum,
            quantifier: false,
        })
    }

    /// A pattern's inline property map. A parameter standing for the whole
    /// map has no typed form.
    fn properties(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        match node.first(grammar::RULE_MAP) {
            Some(map) => self.map(map),
            None => self.refuse(),
        }
    }

    fn map(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let keys = node.rules(grammar::RULE_PROPERTY_KEY_NAME).map(property_key);
        let values: Vec<_> = node.rules(grammar::RULE_EXPRESSION).collect();
        let entries = keys
            .zip(values)
            .map(|(key, value)| Ok((key, self.expression(value)?)))
            .collect::<Lowered<Vec<_>>>()?;
        Ok(Expression::Map(entries))
    }

    fn with_clause(&mut self, node: Cst<'_>) -> Lowered<WithClause> {
        self.context = QueryFailureReason::Return;
        let tail = self.projection(node.required(grammar::RULE_RETURN_BODY)?)?;
        let predicate = match node.first(grammar::RULE_WHERE_CLAUSE) {
            Some(clause) => {
                self.context = QueryFailureReason::Where;
                Some(self.expression(clause.required(grammar::RULE_EXPRESSION)?)?)
            }
            None => None,
        };
        Ok(WithClause {
            table_only: false,
            quantifier: tail.quantifier,
            items: tail.items,
            group_by: tail.group_by,
            order_by: tail.order_by,
            skip: tail.skip,
            limit: tail.limit,
            predicate,
        })
    }

    fn return_clause(&mut self, node: Cst<'_>) -> Lowered<ReturnClause> {
        self.context = QueryFailureReason::Return;
        let tail = self.projection(node.required(grammar::RULE_RETURN_BODY)?)?;
        Ok(ReturnClause {
            quantifier: tail.quantifier,
            items: tail.items,
            group_by: tail.group_by,
            order_by: tail.order_by,
            skip: tail.skip,
            limit: tail.limit,
        })
    }

    /// What `WITH` and `RETURN` have in common: the `returnBody` rule.
    fn projection(&mut self, node: Cst<'_>) -> Lowered<ProjectionTail> {
        // `ALL` is the default set quantifier and means no deduplication;
        // only `DISTINCT` is carried.
        let quantifier = node.has(grammar::DISTINCT).then_some(SetQuantifier::Distinct);
        let items_node = node.required(grammar::RULE_RETURN_ITEMS)?;
        let projections = items_node
            .rules(grammar::RULE_RETURN_ITEM)
            .map(|item| {
                let expression = self.expression(item.required(grammar::RULE_EXPRESSION)?)?;
                // `AS name`, or GQL's `AS "name"` (a delimited identifier).
                let alias = item.first(grammar::RULE_VARIABLE).map(variable_name).or_else(|| {
                    item.tokens()
                        .find(|(kind, _)| *kind == grammar::STRING_LITERAL2)
                        .map(|(_, text)| identifier(text))
                });
                Ok(Projection { expression, alias })
            })
            .collect::<Lowered<Vec<_>>>()?;
        let items = if items_node.has(grammar::TIMES) {
            ProjectionItems::All(projections)
        } else {
            ProjectionItems::Items(projections)
        };

        let group_by = match node.first(grammar::RULE_GROUP_BY) {
            // `GROUP BY ()` and `GROUP BY ALL` have no typed form: an empty
            // key list means "grouping follows from the aggregates".
            Some(group) if group.has(grammar::LPAREN) || group.has(grammar::ALL) => return self.refuse(),
            Some(group) => group
                .rules(grammar::RULE_EXPRESSION)
                .map(|key| self.expression(key))
                .collect::<Lowered<Vec<_>>>()?,
            None => Vec::new(),
        };
        let order_by = match node.first(grammar::RULE_ORDER_BY) {
            Some(order) => self.order_by(order)?,
            None => Vec::new(),
        };
        let skip = node.first(grammar::RULE_SKIP).map(|skip| self.window(skip)).transpose()?;
        let limit = node.first(grammar::RULE_LIMIT).map(|limit| self.window(limit)).transpose()?;
        Ok(ProjectionTail {
            quantifier,
            items,
            group_by,
            order_by,
            skip,
            limit,
        })
    }

    fn order_by(&mut self, node: Cst<'_>) -> Lowered<Vec<SortItem>> {
        self.context = QueryFailureReason::OrderWindow;
        node.rules(grammar::RULE_ORDER_ITEM)
            .map(|item| {
                let expression = self.expression(item.required(grammar::RULE_EXPRESSION)?)?;
                let direction = if item.first(grammar::RULE_ASC_TOKEN).is_some() {
                    Some(OrderDirection::Ascending)
                } else if item.first(grammar::RULE_DESC_TOKEN).is_some() {
                    Some(OrderDirection::Descending)
                } else {
                    None
                };
                let nulls = item.has(grammar::NULLS).then(|| {
                    if item.has(grammar::FIRST) {
                        NullOrdering::First
                    } else {
                        NullOrdering::Last
                    }
                });
                Ok(SortItem {
                    expression,
                    direction,
                    nulls,
                })
            })
            .collect()
    }

    /// `SKIP`/`OFFSET` or `LIMIT`, whose only child expression is the count.
    fn window(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        self.context = QueryFailureReason::OrderWindow;
        self.expression(node.required(grammar::RULE_EXPRESSION)?)
    }

    /// GQL's `<order by and page statement>` (§14.9) between clauses.
    fn order_by_and_page(&mut self, node: Cst<'_>) -> Lowered<OrderByAndPageClause> {
        self.context = QueryFailureReason::OrderWindow;
        let order_by = match node.first(grammar::RULE_ORDER_BY) {
            Some(order) => self.order_by(order)?,
            None => Vec::new(),
        };
        let skip = node.first(grammar::RULE_SKIP).map(|skip| self.window(skip)).transpose()?;
        let limit = node.first(grammar::RULE_LIMIT).map(|limit| self.window(limit)).transpose()?;
        Ok(OrderByAndPageClause { order_by, skip, limit })
    }

    fn let_clause(&mut self, node: Cst<'_>) -> Lowered<LetClause> {
        self.context = QueryFailureReason::Return;
        let bindings = node
            .rules(grammar::RULE_LET_VARIABLE_DEFINITION)
            .map(|item| {
                let item = item.only_rule()?;
                self.value_binding(item)
            })
            .collect::<Lowered<Vec<_>>>()?;
        Ok(LetClause {
            bindings,
            definition: false,
        })
    }

    fn filter_clause(&mut self, node: Cst<'_>) -> Lowered<FilterClause> {
        self.context = QueryFailureReason::Where;
        // `FILTER WHERE p` and `FILTER p` mean the same.
        let predicate = self.expression(node.required(grammar::RULE_EXPRESSION)?)?;
        Ok(FilterClause { predicate })
    }

    fn unwind_clause(&mut self, node: Cst<'_>) -> Lowered<UnwindClause> {
        self.context = QueryFailureReason::Unwind;
        let expression = self.expression(node.required(grammar::RULE_EXPRESSION)?)?;
        let variable = variable_name(node.required(grammar::RULE_VARIABLE)?);
        Ok(UnwindClause {
            expression,
            variable,
            requires_list: false,
            position: None,
        })
    }

    /// GQL's `FOR x IN list`, which is `UNWIND list AS x` over a list only.
    fn for_list_clause(&mut self, node: Cst<'_>) -> Lowered<UnwindClause> {
        self.context = QueryFailureReason::Unwind;
        let mut variables = node.rules(grammar::RULE_VARIABLE).map(variable_name);
        let variable = variables.next().ok_or(QueryFailureReason::UntypedStatement)?;
        let expression = self.expression(node.required(grammar::RULE_EXPRESSION)?)?;
        // §14.8 GQ11, GQ24.
        let position = variables.next().map(|variable| ForPosition {
            variable,
            offset: node.has(grammar::OFFSET),
        });
        Ok(UnwindClause {
            expression,
            variable,
            requires_list: true,
            position,
        })
    }

    // ---- Expressions, one method per precedence level of the grammar. ----

    /// `expression: expression11 (OR expression11)*`, with `expression11:
    /// expression10 (XOR expression10)*`. GQL puts `OR` and `XOR` on one
    /// level, left-associative (§20.20 <boolean value expression>), where the
    /// grammar binds `XOR` tighter, so the two levels are read as one chain:
    /// `a OR b XOR c` is `(a OR b) XOR c`.
    fn expression(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let mut expression: Option<Expression> = None;
        let mut pending = None;
        for child in node.children() {
            match child {
                Child::Token(kind, _) if kind == grammar::OR => pending = Some(BinaryOperator::Or),
                Child::Rule(part) if part.rule() == grammar::RULE_EXPRESSION11 => {
                    for inner in part.children() {
                        match inner {
                            Child::Token(kind, _) if kind == grammar::XOR => pending = Some(BinaryOperator::Xor),
                            Child::Rule(operand) if operand.rule() == grammar::RULE_EXPRESSION10 => {
                                let right = self.expression10(operand)?;
                                expression = Some(match (expression.take(), pending.take()) {
                                    (None, _) => right,
                                    (Some(left), Some(operator)) => Expression::Binary {
                                        left: Box::new(left),
                                        operator,
                                        right: Box::new(right),
                                    },
                                    (Some(_), None) => return Err(QueryFailureReason::UntypedStatement),
                                });
                            }
                            _ => return self.refuse(),
                        }
                    }
                }
                _ => return self.refuse(),
            }
        }
        expression.ok_or(QueryFailureReason::UntypedStatement)
    }

    /// `expression10: expression9 (AND expression9)*`.
    fn expression10(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        self.left_fold(node, grammar::RULE_EXPRESSION9, Self::expression9, |kind| {
            (kind == grammar::AND).then_some(BinaryOperator::And)
        })
    }

    /// `expression9: NOT* expression8`.
    fn expression9(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let negations = node.tokens().filter(|(kind, _)| *kind == grammar::NOT).count();
        let mut expression = self.expression8(node.required(grammar::RULE_EXPRESSION8)?)?;
        for _ in 0..negations {
            expression = Expression::Unary {
                operator: UnaryOperator::Not,
                expression: Box::new(expression),
            };
        }
        Ok(expression)
    }

    /// `expression8: expression7 (comparison expression7)*`. A chain
    /// `a < b < c` means `a < b AND b < c`, each operand read once in the
    /// tree but shared by the two comparisons beside it.
    fn expression8(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let mut left: Option<Expression> = None;
        let mut operator = None;
        let mut chain: Option<Expression> = None;
        for child in node.children() {
            match child {
                Child::Token(kind, _) => {
                    operator = Some(match kind {
                        grammar::EQ => BinaryOperator::Equal,
                        grammar::NEQ | grammar::INVALID_NEQ => BinaryOperator::NotEqual,
                        grammar::LT => BinaryOperator::LessThan,
                        grammar::LE => BinaryOperator::LessThanOrEqual,
                        grammar::GT => BinaryOperator::GreaterThan,
                        grammar::GE => BinaryOperator::GreaterThanOrEqual,
                        _ => return self.refuse(),
                    });
                }
                Child::Rule(operand) => {
                    let right = self.expression7(operand)?;
                    if let (Some(previous), Some(operator)) = (left.take(), operator.take()) {
                        let comparison = Expression::Binary {
                            left: Box::new(previous),
                            operator,
                            right: Box::new(right.clone()),
                        };
                        chain = Some(match chain {
                            None => comparison,
                            Some(chain) => Expression::Binary {
                                left: Box::new(chain),
                                operator: BinaryOperator::And,
                                right: Box::new(comparison),
                            },
                        });
                    }
                    left = Some(right);
                }
            }
        }
        chain.or(left).ok_or(QueryFailureReason::UntypedStatement)
    }

    /// `expression7: expression6 comparisonExpression6?`: the postfix
    /// predicates `IS [NOT] NULL`, `STARTS WITH` and `IN`.
    fn expression7(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let left = self.expression6(node.required(grammar::RULE_EXPRESSION6)?)?;
        match node.first(grammar::RULE_COMPARISON_EXPRESSION6) {
            Some(predicate) => self.postfix_predicate(left, predicate),
            None => Ok(left),
        }
    }

    /// `left` under one `comparisonExpression6`, the postfix predicate shared
    /// by `expression7` and the `WHEN` operands of a simple CASE.
    fn postfix_predicate(&mut self, left: Expression, predicate: Cst<'_>) -> Lowered<Expression> {
        // §19.6 GA06: `IS [NOT] TYPED T` and `IS [NOT] :: T`, and Cypher's
        // bare `:: T`.
        if let Some(value_type) = predicate.first(grammar::RULE_TYPE) {
            return Ok(Expression::TypeTest {
                expression: Box::new(left),
                value_type: self.value_type(value_type)?,
                negated: predicate.has(grammar::NOT),
            });
        }
        if predicate.has(grammar::NULL) && !predicate.has(grammar::NORMALIZED) && predicate.first(grammar::RULE_TYPE).is_none() {
            let operator = if predicate.has(grammar::NOT) {
                UnaryOperator::IsNotNull
            } else {
                UnaryOperator::IsNull
            };
            return Ok(Expression::Unary {
                operator,
                expression: Box::new(left),
            });
        }
        if predicate.has(grammar::NORMALIZED) {
            let form = predicate
                .first(grammar::RULE_NORMAL_FORM)
                .map_or_else(|| "NFC".to_string(), |form| form.text().to_ascii_uppercase());
            return Ok(Expression::Normalized {
                expression: Box::new(left),
                form: form.into(),
                negated: predicate.has(grammar::NOT),
            });
        }
        // §19.10 G112.
        if predicate.has(grammar::OF) {
            let edge = variable_name(predicate.required(grammar::RULE_VARIABLE)?);
            let edge = Box::new(Expression::Identifier(edge));
            let test = if predicate.has(grammar::SOURCE) {
                ElementTest::SourceOf(edge)
            } else {
                ElementTest::DestinationOf(edge)
            };
            return self.element_test(left, test, predicate.has(grammar::NOT));
        }
        if let Some(labels) = predicate.first(grammar::RULE_LABEL_EXPRESSION4) {
            // GQL reserves TRUE, FALSE and UNKNOWN, so `x IS TRUE` is the
            // boolean test of §20.20; a backticked `` x IS `TRUE` `` stays a
            // label predicate.
            if let (true, false, Some(truth)) = (predicate.has(grammar::IS), predicate.has(grammar::LABELED), truth_value(labels)) {
                let negated = predicate.has(grammar::NOT);
                let operator = match (truth, negated) {
                    (Truth::True, false) => UnaryOperator::IsTrue,
                    (Truth::True, true) => UnaryOperator::IsNotTrue,
                    (Truth::False, false) => UnaryOperator::IsFalse,
                    (Truth::False, true) => UnaryOperator::IsNotFalse,
                    (Truth::Unknown, false) => UnaryOperator::IsUnknown,
                    (Truth::Unknown, true) => UnaryOperator::IsNotUnknown,
                };
                return Ok(Expression::Unary {
                    operator,
                    expression: Box::new(left),
                });
            }
            // §19.8 G110: `e IS [NOT] DIRECTED`. GQL does not reserve
            // DIRECTED, but the engine never ran Cypher's label test here, so
            // the GQL reading wins; a backticked `` e IS `DIRECTED` `` stays a
            // label test.
            let negated = predicate.has(grammar::NOT);
            if predicate.has(grammar::IS) && !predicate.has(grammar::LABELED) && bare_word(labels).as_deref() == Some("DIRECTED") {
                return self.element_test(left, ElementTest::Directed, negated);
            }
            // §19.9 G111: `x IS [NOT] LABELED L` and `x:L`. Cypher's `x IS L`
            // means the same.
            let labels = self.label_expression(labels)?;
            return self.element_test(left, ElementTest::Labeled(labels), negated);
        }
        let operator = if predicate.has(grammar::STARTS) {
            BinaryOperator::StartsWith
        } else if predicate.has(grammar::ENDS) {
            BinaryOperator::EndsWith
        } else if predicate.has(grammar::CONTAINS) {
            BinaryOperator::Contains
        } else if predicate.has(grammar::IN) {
            BinaryOperator::In
        } else {
            // `=~`, type, normal-form and label predicates.
            return self.refuse();
        };
        let right = self.expression6(predicate.required(grammar::RULE_EXPRESSION6)?)?;
        Ok(Expression::Binary {
            left: Box::new(left),
            operator,
            right: Box::new(right),
        })
    }

    /// `expression6: expression5 ((PLUS | MINUS | DOUBLEBAR) expression5)*`.
    fn expression6(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        self.left_fold(node, grammar::RULE_EXPRESSION5, Self::expression5, |kind| match kind {
            grammar::PLUS => Some(BinaryOperator::Add),
            grammar::MINUS => Some(BinaryOperator::Subtract),
            grammar::DOUBLEBAR => Some(BinaryOperator::Concatenate),
            _ => None,
        })
    }

    /// `expression5: expression4 ((TIMES | DIVIDE | PERCENT) expression4)*`.
    fn expression5(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        self.left_fold(node, grammar::RULE_EXPRESSION4, Self::expression4, |kind| match kind {
            grammar::TIMES => Some(BinaryOperator::Multiply),
            grammar::DIVIDE => Some(BinaryOperator::Divide),
            grammar::PERCENT => Some(BinaryOperator::Modulo),
            _ => None,
        })
    }

    /// `expression4: expression3 (POW expression3)*`. Exponentiation has no
    /// executor yet.
    fn expression4(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        self.left_fold(node, grammar::RULE_EXPRESSION3, Self::expression3, |_| None)
    }

    /// `expression3: expression2 | (PLUS | MINUS) expression2`.
    fn expression3(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let expression = self.expression2(node.required(grammar::RULE_EXPRESSION2)?)?;
        let operator = if node.has(grammar::MINUS) {
            UnaryOperator::Minus
        } else if node.has(grammar::PLUS) {
            UnaryOperator::Plus
        } else {
            return Ok(expression);
        };
        Ok(Expression::Unary {
            operator,
            expression: Box::new(expression),
        })
    }

    /// `expression2: expression1 postFix*`. Property reads have a typed
    /// form, `n.name` and chains of them; the engine decides which sources
    /// have properties. An index, `list[i]`, is element access; a slice,
    /// `list[a..b]`, is not supported and is refused.
    fn expression2(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let mut expression = self.expression1(node.required(grammar::RULE_EXPRESSION1)?)?;
        for postfix in node.rules(grammar::RULE_POST_FIX) {
            let Some(property) = postfix.first(grammar::RULE_PROPERTY) else {
                // `IndexPostfix`: `[ expression ]`. `RangePostfix` also opens
                // with `[` but carries `..`.
                if postfix.has(grammar::DOTDOT) {
                    return self.refuse();
                }
                let index = self.expression(postfix.required(grammar::RULE_EXPRESSION)?)?;
                expression = Expression::Index {
                    target: Box::new(expression),
                    index: Box::new(index),
                };
                continue;
            };
            // Any expression may stand before the period: the planner reads
            // a record's field (§20.11) and refuses what has none.
            expression = Expression::Property {
                target: Box::new(expression),
                key: property_key(property.required(grammar::RULE_PROPERTY_KEY_NAME)?),
            };
        }
        Ok(expression)
    }

    fn expression1(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let primary = node.only_rule()?;
        match primary.rule() {
            grammar::RULE_LITERAL => self.literal(primary),
            grammar::RULE_PARAMETER => Ok(Expression::Parameter(parameter_name(primary)?)),
            grammar::RULE_VARIABLE => {
                // GQL's `UNKNOWN` (the null boolean) and `SESSION_USER`
                // (§20.3), written bare. A variable may also be spelled so in
                // Cypher; which one the word is depends on what is in scope
                // where it stands, which the planner decides.
                let text = primary.text();
                // GQL's `UNKNOWN` (the null boolean), `SESSION_USER` (§20.3)
                // and the current date and time functions without
                // parentheses (§20.27), written bare. A variable may also be
                // spelled so in Cypher; which one the word is depends on what
                // is in scope where it stands, which the planner decides.
                Ok(match ValueKeyword::from_word(&text) {
                    Some(keyword) => Expression::ValueKeyword {
                        keyword,
                        spelling: variable_name(primary),
                    },
                    None => Expression::Identifier(variable_name(primary)),
                })
            }
            grammar::RULE_PARENTHESIZED_EXPRESSION => self.expression(primary.required(grammar::RULE_EXPRESSION)?),
            grammar::RULE_LIST_LITERAL => Ok(Expression::List(
                primary
                    .rules(grammar::RULE_EXPRESSION)
                    .map(|element| self.expression(element))
                    .collect::<Lowered<Vec<_>>>()?,
            )),
            grammar::RULE_COUNT_STAR => Ok(Expression::Function {
                name: vec![Identifier::from("count")],
                distinct: false,
                arguments: vec![Expression::Syntax(SyntaxFragment::new("*"))],
            }),
            grammar::RULE_FUNCTION_INVOCATION => self.function(primary),
            grammar::RULE_EXISTS_EXPRESSION => self.exists(primary),
            // §20.6 GQ18: the subquery must end in RETURN.
            grammar::RULE_VALUE_QUERY_EXPRESSION => {
                let context = self.context;
                let open_query = std::mem::replace(&mut self.open_query, false);
                let query = self.query_with_local_definitions(primary.required(grammar::RULE_QUERY_WITH_LOCAL_DEFINITIONS)?);
                self.open_query = open_query;
                self.context = context;
                Ok(Expression::Value(Box::new(query?)))
            }
            // §20.8 GA05.
            grammar::RULE_RECORD_CONSTRUCTOR => self.map(primary.required(grammar::RULE_MAP)?),
            // §20.5 GE03.
            grammar::RULE_LET_VALUE_EXPRESSION => {
                let bindings = primary
                    .rules(grammar::RULE_LET_ITEM)
                    .map(|item| {
                        Ok((
                            variable_name(item.required(grammar::RULE_VARIABLE)?),
                            self.expression(item.required(grammar::RULE_EXPRESSION)?)?,
                        ))
                    })
                    .collect::<Lowered<Vec<_>>>()?;
                Ok(Expression::Let {
                    bindings,
                    body: Box::new(self.expression(primary.required(grammar::RULE_EXPRESSION)?)?),
                })
            }
            // §20.14 GE06.
            grammar::RULE_PATH_CONSTRUCTOR => Ok(Expression::PathConstructor(
                primary
                    .rules(grammar::RULE_EXPRESSION)
                    .map(|element| self.expression(element))
                    .collect::<Lowered<Vec<_>>>()?,
            )),
            // §21.2 GV39 to GV41.
            grammar::RULE_TEMPORAL_LITERAL => {
                let kind = if primary.has(grammar::DATE) {
                    TemporalLiteralKind::Date
                } else if primary.has(grammar::TIME) {
                    TemporalLiteralKind::Time
                } else if primary.has(grammar::DURATION) {
                    TemporalLiteralKind::Duration
                } else if primary.has(grammar::TIMESTAMP) {
                    TemporalLiteralKind::Timestamp
                } else if primary.has(grammar::DATETIME) {
                    TemporalLiteralKind::Datetime
                } else {
                    TemporalLiteralKind::Interval(self.interval_qualifier(primary.required(grammar::RULE_INTERVAL_QUALIFIER)?)?)
                };
                let mut text = unquote_string(&primary.required(grammar::RULE_STRING_LITERAL)?.text());
                // GL12: the sign before an interval's string leads its text.
                if matches!(kind, TemporalLiteralKind::Interval(_)) {
                    text.insert(0, if primary.has(grammar::MINUS) { '-' } else { '+' });
                }
                Ok(Expression::TemporalLiteral { kind, text: text.into() })
            }
            // §20.28 GV41: the qualifier, when written, as a third argument.
            grammar::RULE_DURATION_BETWEEN => {
                let mut arguments = primary
                    .rules(grammar::RULE_EXPRESSION)
                    .map(|argument| self.expression(argument))
                    .collect::<Lowered<Vec<_>>>()?;
                if let Some(qualifier) = primary.first(grammar::RULE_TEMPORAL_DURATION_QUALIFIER) {
                    let qualifier = if qualifier.has(grammar::YEAR) {
                        "YEAR TO MONTH"
                    } else {
                        "DAY TO SECOND"
                    };
                    arguments.push(Expression::Literal(Literal::String(qualifier.into())));
                }
                Ok(Expression::Function {
                    name: vec![Identifier::new("duration_between")],
                    distinct: false,
                    arguments,
                })
            }
            grammar::RULE_CAST_SPECIFICATION => Ok(Expression::Cast {
                expression: Box::new(self.expression(primary.required(grammar::RULE_EXPRESSION)?)?),
                value_type: self.value_type(primary.required(grammar::RULE_TYPE)?)?,
            }),
            grammar::RULE_TRIM_FUNCTION => self.trim(primary),
            grammar::RULE_NORMALIZE_FUNCTION => self.normalize(primary),
            // §19.13 G115.
            grammar::RULE_PROPERTY_EXISTS_PREDICATE => {
                let element = Expression::Identifier(variable_name(primary.required(grammar::RULE_VARIABLE)?));
                let property = property_key(primary.required(grammar::RULE_PROPERTY_KEY_NAME)?);
                self.element_test(element, ElementTest::PropertyExists(property), false)
            }
            grammar::RULE_CASE_EXPRESSION => self.searched_case(primary),
            grammar::RULE_EXTENDED_CASE_EXPRESSION => self.simple_case(primary),
            _ => self.refuse(),
        }
    }

    fn literal(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        if let Some(number) = node.first(grammar::RULE_NUMBER_LITERAL) {
            return self.number(number);
        }
        if let Some(string) = node.first(grammar::RULE_STRING_LITERAL) {
            return Ok(Expression::Literal(Literal::String(unquote_string(&string.text()).into())));
        }
        if let Some(sequence) = node.first(grammar::RULE_BYTE_STRING_SEQUENCE) {
            return self.byte_string(sequence).map(|bytes| Expression::Literal(Literal::Bytes(bytes)));
        }
        let literal = if node.has(grammar::TRUE) {
            Literal::Boolean(true)
        } else if node.has(grammar::FALSE) {
            Literal::Boolean(false)
        } else if node.has(grammar::NULL) {
            Literal::Null
        } else if let Some(map) = node.first(grammar::RULE_MAP) {
            // §20.18 GV45: a map literal is a record constructor.
            return self.map(map);
        } else {
            // `INF`, `INFINITY` and `NAN`.
            return self.refuse();
        };
        Ok(Expression::Literal(literal))
    }

    fn byte_string(&self, sequence: Cst<'_>) -> Lowered<Box<[u8]>> {
        let malformed = QueryFailureReason::ParseError;
        let mut bytes = Vec::new();
        let mut previous_end = None;
        for terminal in sequence.0.children().filter_map(|node| node.as_terminal()) {
            let symbol = terminal.symbol();
            let span = symbol.byte_span().ok_or(malformed)?;
            if let Some(end) = previous_end {
                // §21.2 SR57; the grammar has already restricted this gap to
                // hidden-channel separators, including comments (§21.3 SR15–16).
                if !self.source[end..span.start].contains(['\r', '\n']) {
                    return Err(malformed);
                }
            }
            previous_end = Some(span.end);
            let text = terminal.text();
            let piece = if symbol.token_type() == grammar::BYTE_STRING_LITERAL {
                &text[1..]
            } else {
                text
            };
            let digits: String = piece[1..piece.len() - 1].chars().filter(|character| *character != ' ').collect();
            if !digits.len().is_multiple_of(2) || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(malformed);
            }
            for pair in digits.as_bytes().as_chunks::<2>().0 {
                let digit = |byte: u8| (byte as char).to_digit(16).ok_or(malformed);
                bytes.push((digit(pair[0])? * 16 + digit(pair[1])?) as u8);
            }
        }
        Ok(bytes.into())
    }

    /// `numberLiteral: MINUS? (DECIMAL_DOUBLE | UNSIGNED_DECIMAL_INTEGER |
    /// UNSIGNED_HEX_INTEGER | UNSIGNED_OCTAL_INTEGER)`. A leading minus stays
    /// a unary operator over the unsigned value.
    fn number(&self, node: Cst<'_>) -> Lowered<Expression> {
        let mut negative = false;
        let mut literal = None;
        for (kind, text) in node.tokens() {
            match kind {
                grammar::MINUS => negative = true,
                // The lexer lets letters into these tokens; a digit the radix
                // lacks makes the text no token at all (§21.2), a syntax error.
                // §21.2 GL05, GL06: an exact number with the suffix `M`.
                // Any other spelling with the suffix is no literal at all.
                grammar::UNSIGNED_DECIMAL_INTEGER | grammar::DECIMAL_DOUBLE if text.ends_with(['m', 'M']) => {
                    literal = Some(Literal::Decimal(exact_stem(text).ok_or(QueryFailureReason::ParseError)?.into()));
                }
                grammar::UNSIGNED_DECIMAL_INTEGER => {
                    literal = Some(match unsigned_integer(kind, text) {
                        Some(value) => Literal::Integer(value),
                        // §21.2 GL07: `5F` and `5D` are approximate numbers.
                        None => Literal::Float(
                            approximate_stem(text)
                                // The stem is an unsigned decimal integer of any size:
                                // FLOAT's range, not an integer's, bounds it.
                                .filter(|stem| is_decimal_integer(stem))
                                .and_then(float_spelling)
                                .ok_or(QueryFailureReason::ParseError)?
                                .into(),
                        ),
                    })
                }
                grammar::UNSIGNED_HEX_INTEGER | grammar::UNSIGNED_OCTAL_INTEGER | grammar::UNSIGNED_BINARY_INTEGER => {
                    literal = Some(Literal::Integer(
                        unsigned_integer(kind, text).ok_or(QueryFailureReason::ParseError)?,
                    ))
                }
                grammar::DECIMAL_DOUBLE => {
                    // §21.2 GL07 to GL10: `1.5F`, `1.5E3d`. FLOAT is binary64,
                    // so the regular (`F`) and double (`D`) approximate types
                    // are both FLOAT.
                    let spelling = float_spelling(text).or_else(|| approximate_stem(text).and_then(float_spelling));
                    literal = Some(Literal::Float(spelling.ok_or(self.context)?.into()))
                }
                _ => return self.refuse(),
            }
        }
        let literal = Expression::Literal(literal.ok_or(self.context)?);
        Ok(if negative {
            Expression::Unary {
                operator: UnaryOperator::Minus,
                expression: Box::new(literal),
            }
        } else {
            literal
        })
    }

    fn function(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let function_name = node.required(grammar::RULE_FUNCTION_NAME)?;
        let mut name: Vec<Identifier> = function_name
            .required(grammar::RULE_NAMESPACE)?
            .rules(grammar::RULE_SYMBOLIC_NAME_STRING)
            .map(symbolic_name)
            .collect();
        name.push(symbolic_name(function_name.required(grammar::RULE_SYMBOLIC_NAME_STRING)?));
        let arguments = node
            .rules(grammar::RULE_FUNCTION_ARGUMENT)
            .map(|argument| self.expression(argument.required(grammar::RULE_EXPRESSION)?))
            .collect::<Lowered<Vec<_>>>()?;
        Ok(Expression::Function {
            name,
            // `ALL` is the default set quantifier.
            distinct: node.has(grammar::DISTINCT),
            arguments,
        })
    }

    /// `EXISTS { pattern [WHERE ...] }`, `EXISTS ( pattern [WHERE ...] )` and
    /// `EXISTS { query }` (§19.4).
    fn exists(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let context = self.context;
        let open_query = std::mem::replace(&mut self.open_query, true);
        let query = if let Some(query) = node.first(grammar::RULE_QUERY_WITH_LOCAL_DEFINITIONS) {
            self.query_with_local_definitions(query)
        } else {
            self.exists_pattern(node)
        };
        self.open_query = open_query;
        self.context = context;
        Ok(Expression::Exists(Box::new(query?)))
    }

    /// The pattern form of `EXISTS`, which is the query of one MATCH.
    fn exists_pattern(&mut self, node: Cst<'_>) -> Lowered<Query> {
        self.context = QueryFailureReason::Pattern;
        let mode = node.first(grammar::RULE_MATCH_MODE).map(|mode| {
            if mode.has(grammar::REPEATABLE) {
                MatchMode::RepeatableElements
            } else {
                MatchMode::DifferentRelationships
            }
        });
        let patterns = node
            .required(grammar::RULE_PATTERN_LIST)?
            .rules(grammar::RULE_PATTERN)
            .map(|pattern| self.pattern(pattern))
            .collect::<Lowered<Vec<_>>>()?;
        let predicate = match node.first(grammar::RULE_WHERE_CLAUSE) {
            Some(clause) => {
                self.context = QueryFailureReason::Where;
                Some(self.expression(clause.required(grammar::RULE_EXPRESSION)?)?)
            }
            None => None,
        };
        Ok(Query {
            clauses: vec![Clause::Match(MatchClause {
                optional: false,
                mode,
                patterns,
                predicate,
                keep: None,
                yield_items: None,
            })],
            unions: Vec::new(),
        })
    }

    /// `TRIM([[BOTH | LEADING | TRAILING] [character] FROM] source)`, lowered
    /// to `trim(source)` or, with `FROM`, `trim(source, character, side)`:
    /// the character defaults to a space and the side to `BOTH`. `FROM` with
    /// neither a side nor a character breaks §20.24 Syntax Rule 3; it lowers
    /// with an empty side, which the engine reports.
    fn trim(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let operands: Vec<_> = node.rules(grammar::RULE_EXPRESSION).collect();
        let name = vec![Identifier::from("trim")];
        if !node.has(grammar::FROM) {
            let [source] = operands.as_slice() else {
                return Err(QueryFailureReason::UntypedStatement);
            };
            return Ok(Expression::Function {
                name,
                distinct: false,
                arguments: vec![self.expression(*source)?],
            });
        }
        let (character, source) = match operands.as_slice() {
            [source] => (Expression::Literal(Literal::String(" ".into())), *source),
            [character, source] => (self.expression(*character)?, *source),
            _ => return Err(QueryFailureReason::UntypedStatement),
        };
        let side = if node.has(grammar::LEADING) {
            "LEADING"
        } else if node.has(grammar::TRAILING) {
            "TRAILING"
        } else if node.has(grammar::BOTH) || operands.len() == 2 {
            "BOTH"
        } else {
            ""
        };
        Ok(Expression::Function {
            name,
            distinct: false,
            arguments: vec![
                self.expression(source)?,
                character,
                Expression::Literal(Literal::String(side.into())),
            ],
        })
    }

    /// `NORMALIZE(s [, form])`, lowered to `normalize(s, 'NFC')` with the
    /// form spelled as a string.
    fn normalize(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let source = self.expression(node.required(grammar::RULE_EXPRESSION)?)?;
        let form = node
            .first(grammar::RULE_NORMAL_FORM)
            .map_or_else(|| "NFC".to_string(), |form| form.text().to_ascii_uppercase());
        Ok(Expression::Function {
            name: vec![Identifier::from("normalize")],
            distinct: false,
            arguments: vec![source, Expression::Literal(Literal::String(form.into()))],
        })
    }

    /// `CASE WHEN condition THEN result ... [ELSE default] END`.
    fn searched_case(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let branches = node
            .rules(grammar::RULE_CASE_ALTERNATIVE)
            .map(|alternative| {
                let mut parts = alternative.rules(grammar::RULE_EXPRESSION);
                let (Some(when), Some(then)) = (parts.next(), parts.next()) else {
                    return Err(QueryFailureReason::UntypedStatement);
                };
                Ok((self.expression(when)?, self.expression(then)?))
            })
            .collect::<Lowered<Vec<_>>>()?;
        let default = node
            .first(grammar::RULE_EXPRESSION)
            .map(|default| self.expression(default))
            .transpose()?
            .map(Box::new);
        Ok(Expression::Case {
            operand: None,
            branches,
            default,
        })
    }

    /// `CASE operand WHEN ... THEN result ... [ELSE default] END` (§20.7).
    ///
    /// When every `WHEN` is one plain value, the operand is kept and compared
    /// with `=`. Otherwise -- a list `WHEN 1, 2`, a comparison `WHEN > 5`, a
    /// postfix predicate `WHEN IS NULL` -- the CASE becomes the searched CASE
    /// the standard defines it as: each `WHEN` is the operand under that
    /// predicate, and a list is their disjunction.
    fn simple_case(&mut self, node: Cst<'_>) -> Lowered<Expression> {
        let mut expressions = node.rules(grammar::RULE_EXPRESSION);
        let operand = self.expression(expressions.next().ok_or(QueryFailureReason::UntypedStatement)?)?;
        let default = expressions
            .next()
            .map(|default| self.expression(default))
            .transpose()?
            .map(Box::new);
        let alternatives: Vec<_> = node.rules(grammar::RULE_EXTENDED_CASE_ALTERNATIVE).collect();
        let plain = alternatives.iter().all(|alternative| {
            let mut whens = alternative.rules(grammar::RULE_EXTENDED_WHEN);
            matches!((whens.next(), whens.next()), (Some(when), None) if when_value(when).is_some())
        });
        let mut branches = Vec::with_capacity(alternatives.len());
        for alternative in alternatives {
            let then = self.expression(alternative.required(grammar::RULE_EXPRESSION)?)?;
            let mut condition: Option<Expression> = None;
            for when in alternative.rules(grammar::RULE_EXTENDED_WHEN) {
                let test = if plain {
                    self.expression(when_value(when).ok_or(QueryFailureReason::UntypedStatement)?)?
                } else {
                    self.when_condition(&operand, when)?
                };
                condition = Some(match condition {
                    None => test,
                    Some(previous) => Expression::Binary {
                        left: Box::new(previous),
                        operator: BinaryOperator::Or,
                        right: Box::new(test),
                    },
                });
            }
            branches.push((condition.ok_or(QueryFailureReason::UntypedStatement)?, then));
        }
        Ok(Expression::Case {
            operand: plain.then(|| Box::new(operand)),
            branches,
            default,
        })
    }

    /// One `extendedWhen` as a condition on the CASE operand.
    fn when_condition(&mut self, operand: &Expression, when: Cst<'_>) -> Lowered<Expression> {
        if let Some(value) = when_value(when) {
            return Ok(Expression::Binary {
                left: Box::new(operand.clone()),
                operator: BinaryOperator::Equal,
                right: Box::new(self.expression(value)?),
            });
        }
        if let Some(predicate) = when.first(grammar::RULE_COMPARISON_EXPRESSION6) {
            return self.postfix_predicate(operand.clone(), predicate);
        }
        let operator = match when.tokens().next().map(|(kind, _)| kind) {
            Some(grammar::EQ) => BinaryOperator::Equal,
            Some(grammar::NEQ | grammar::INVALID_NEQ) => BinaryOperator::NotEqual,
            Some(grammar::LT) => BinaryOperator::LessThan,
            Some(grammar::LE) => BinaryOperator::LessThanOrEqual,
            Some(grammar::GT) => BinaryOperator::GreaterThan,
            Some(grammar::GE) => BinaryOperator::GreaterThanOrEqual,
            _ => return self.refuse(),
        };
        let right = self.expression7(when.required(grammar::RULE_EXPRESSION7)?)?;
        Ok(Expression::Binary {
            left: Box::new(operand.clone()),
            operator,
            right: Box::new(right),
        })
    }

    /// Fold `operand (operator operand)*` to the left. `operator` maps a token
    /// kind to the binary operator it spells, or `None` for a token this
    /// adapter does not lower, which stops lowering.
    /// A predicate on an element (§19.8 to §19.10, §19.13), whose subject
    /// must be an element variable reference: a variable, not an expression.
    fn element_test(&mut self, subject: Expression, test: ElementTest, negated: bool) -> Lowered<Expression> {
        match subject {
            element @ Expression::Identifier(_) => Ok(Expression::ElementTest {
                element: Box::new(element),
                test,
                negated,
            }),
            _ => self.refuse(),
        }
    }

    fn left_fold(
        &mut self,
        node: Cst<'_>,
        operand_rule: usize,
        mut operand: impl FnMut(&mut Self, Cst<'_>) -> Lowered<Expression>,
        operator: impl Fn(i32) -> Option<BinaryOperator>,
    ) -> Lowered<Expression> {
        let mut expression: Option<Expression> = None;
        let mut pending = None;
        for child in node.children() {
            match child {
                Child::Token(kind, _) => match operator(kind) {
                    Some(operator) => pending = Some(operator),
                    None => return self.refuse(),
                },
                Child::Rule(part) if part.rule() == operand_rule => {
                    let right = operand(self, part)?;
                    expression = Some(match (expression.take(), pending.take()) {
                        (None, _) => right,
                        (Some(left), Some(operator)) => Expression::Binary {
                            left: Box::new(left),
                            operator,
                            right: Box::new(right),
                        },
                        (Some(_), None) => return Err(QueryFailureReason::UntypedStatement),
                    });
                }
                Child::Rule(_) => return self.refuse(),
            }
        }
        expression.ok_or(QueryFailureReason::UntypedStatement)
    }
}

#[derive(Clone, Copy)]
enum Truth {
    True,
    False,
    Unknown,
}

/// The truth value a label expression spells when it is one unquoted word
/// `TRUE`, `FALSE` or `UNKNOWN`.
fn truth_value(labels: Cst<'_>) -> Option<Truth> {
    match bare_word(labels)?.as_str() {
        "TRUE" => Some(Truth::True),
        "FALSE" => Some(Truth::False),
        "UNKNOWN" => Some(Truth::Unknown),
        _ => None,
    }
}

/// The word a label expression is, upper-cased, when it is one unquoted name:
/// a backticked name is never a keyword.
fn bare_word(labels: Cst<'_>) -> Option<String> {
    let mut terms = labels.rules(grammar::RULE_LABEL_EXPRESSION3);
    let (Some(term), None) = (terms.next(), terms.next()) else {
        return None;
    };
    let mut factors = term.rules(grammar::RULE_LABEL_EXPRESSION2);
    let (Some(factor), None) = (factors.next(), factors.next()) else {
        return None;
    };
    if factor.has(grammar::EXCLAMATION_MARK) {
        return None;
    }
    let name = factor
        .first(grammar::RULE_LABEL_EXPRESSION1)?
        .first(grammar::RULE_SYMBOLIC_NAME_STRING)?
        .text();
    (!name.starts_with('`') && !name.starts_with('"')).then(|| name.to_ascii_uppercase())
}

/// The value of a `WHEN value` operand, when that is what `when` is: no
/// comparison operator in front and no postfix predicate.
fn when_value(when: Cst<'_>) -> Option<Cst<'_>> {
    if when.tokens().next().is_some() || when.first(grammar::RULE_COMPARISON_EXPRESSION6).is_some() {
        return None;
    }
    when.first(grammar::RULE_EXPRESSION)
}

/// What `WITH` and `RETURN` have in common, lowered once.
struct ProjectionTail {
    quantifier: Option<SetQuantifier>,
    items: ProjectionItems,
    group_by: Vec<Expression>,
    order_by: Vec<SortItem>,
    skip: Option<Expression>,
    limit: Option<Expression>,
}

/// §16.7 G037, §22.3 GR3 d): bounded set-union expansion of questioned
/// primaries. Skipped primaries contribute neither elements nor predicates.
fn questioned_union(factors: Vec<(PathPattern, bool)>) -> Lowered<PathPattern> {
    const MAXIMUM_QUESTIONED: usize = 3;
    let count = factors.iter().filter(|(_, questioned)| *questioned).count();
    if count > MAXIMUM_QUESTIONED {
        return Err(QueryFailureReason::Pattern);
    }
    let mut operands = Vec::new();
    for skipped in 0..1_usize << count {
        let mut index = 0;
        let mut path = path_elements(Vec::new());
        let mut predicates = Vec::new();
        for (factor, questioned) in &factors {
            if *questioned {
                let skip = skipped & (1 << index) != 0;
                index += 1;
                if skip {
                    continue;
                }
            }
            let mut factor = factor.clone();
            if let Some(predicate) = factor.predicate.take() {
                predicates.push(predicate);
            }
            path = concatenate_paths(path, factor)?;
        }
        finish_path(&mut path)?;
        let predicate = predicates.into_iter().reduce(|left, right| Expression::Binary {
            left: Box::new(left),
            operator: BinaryOperator::And,
            right: Box::new(right),
        });
        operands.push(PathOperand { path, predicate });
    }
    let mut path = path_elements(Vec::new());
    path.alternatives = Some(PathAlternatives { multiset: false, operands });
    Ok(path)
}

fn variable_name(node: Cst<'_>) -> Identifier {
    identifier(&node.text())
}

fn symbolic_name(node: Cst<'_>) -> Identifier {
    identifier(&node.text())
}

fn property_key(node: Cst<'_>) -> Identifier {
    identifier(&node.text())
}

/// `$name`, `$0` or `` $`odd name` ``.
fn parameter_name(node: Cst<'_>) -> Lowered<Identifier> {
    Ok(identifier(&node.required(grammar::RULE_PARAMETER_NAME)?.text()))
}

/// A name as written, without its delimiters: a doubled backtick inside
/// backticks is one backtick, and a double-quoted name (GQL §21.3, from the
/// grammar overlay) reads as a double-quoted string does.
fn identifier(text: &str) -> Identifier {
    if text.len() >= 2 && text.starts_with('"') && text.ends_with('"') {
        return Identifier::from(unquote_string(text));
    }
    match text.strip_prefix('`').and_then(|inner| inner.strip_suffix('`')) {
        Some(inner) => Identifier::from(inner.replace("``", "`")),
        None => Identifier::from(text),
    }
}

/// The value of a quoted string token. The grammar guarantees the quotes and
/// that every backslash starts an escape. A doubled quote of the delimiting
/// kind is one quote (GQL §21.2). `@'...'` (GL11) turns escaping off.
fn unquote_string(text: &str) -> String {
    let (text, escaping) = match text.strip_prefix('@') {
        Some(text) => (text, false),
        None => (text, true),
    };
    let quote = text.chars().next().unwrap_or('\'');
    let inner = &text[1..text.len() - 1];
    let mut value = String::with_capacity(inner.len());
    let mut characters = inner.chars().peekable();
    while let Some(character) = characters.next() {
        if character == quote && characters.peek() == Some(&quote) {
            characters.next();
            value.push(quote);
            continue;
        }
        if character != '\\' || !escaping {
            value.push(character);
            continue;
        }
        let Some(escaped) = characters.next() else {
            break;
        };
        match escaped {
            'n' => value.push('\n'),
            'r' => value.push('\r'),
            't' => value.push('\t'),
            'b' => value.push('\u{8}'),
            'f' => value.push('\u{c}'),
            'u' | 'U' => {
                let width = if escaped == 'u' { 4 } else { 8 };
                let hex: String = characters.clone().take(width).collect();
                match (hex.len() == width)
                    .then(|| u32::from_str_radix(&hex, 16).ok())
                    .flatten()
                    .and_then(char::from_u32)
                {
                    Some(decoded) => {
                        value.push(decoded);
                        for _ in 0..width {
                            characters.next();
                        }
                    }
                    None => value.push(escaped),
                }
            }
            // `\\`, `\'`, `\"` and any other escaped character stand for
            // themselves.
            other => value.push(other),
        }
    }
    value
}

/// The length in `[n]` after a list type name.
fn type_length(node: Cst<'_>) -> Lowered<u64> {
    let text = node.text();
    digits(text.trim_start_matches('[').trim_end_matches(']'))
        .and_then(|value| u64::try_from(value).ok())
        .ok_or(QueryFailureReason::ParseError)
}

/// A label expression as a relationship pattern holds it: a plain
/// disjunction of names as its type list, anything else as itself.
fn type_list(expression: LabelExpression) -> (Vec<Identifier>, Option<LabelExpression>) {
    match expression {
        LabelExpression::Label(name) => (vec![name], None),
        LabelExpression::Or(terms) if terms.iter().all(|term| matches!(term, LabelExpression::Label(_))) => (
            terms
                .into_iter()
                .map(|term| match term {
                    LabelExpression::Label(name) => name,
                    _ => unreachable!("checked above"),
                })
                .collect(),
            None,
        ),
        other => (Vec::new(), Some(other)),
    }
}

/// An edge a simplified path pattern or an abbreviated undirected edge
/// makes: no variable, the given direction and label.
fn simplified_edge(direction: Direction, label: Option<LabelExpression>) -> RelationshipPattern {
    let (types, label_expression) = label.map(type_list).unwrap_or_default();
    RelationshipPattern {
        variable: None,
        types,
        label_expression,
        direction,
        length: None,
        properties: None,
        predicate: None,
        iteration_nodes: None,
    }
}

fn simplified_override(node: Cst<'_>) -> Option<Direction> {
    match (
        node.has(grammar::LT),
        node.has(grammar::TILDE),
        node.has(grammar::GT),
        node.has(grammar::MINUS),
    ) {
        (true, false, true, _) => Some(Direction::Bidirectional),
        (true, true, _, _) => Some(Direction::LeftOrUndirected),
        (true, false, false, _) => Some(Direction::Incoming),
        (false, true, true, _) => Some(Direction::UndirectedOrRight),
        (false, true, false, _) => Some(Direction::UndirectedEdge),
        (false, false, true, _) => Some(Direction::Outgoing),
        (false, false, false, true) => Some(Direction::Undirected),
        (false, false, false, false) => None,
    }
}

/// §16.12 SR6: label algebra stays one edge; path algebra uses the same
/// concatenation and alternatives as ordinary parenthesized paths.
fn simplified_adapter(expression: &hydradb_cypher_ast::SimplifiedPath, direction: Direction) -> Lowered<PathPattern> {
    use hydradb_cypher_ast::SimplifiedPath as S;
    if let Ok(label) = simplified_label(expression) {
        return Ok(path_elements(vec![PatternElement::Relationship(simplified_edge(
            direction,
            Some(label),
        ))]));
    }
    match expression {
        S::DirectionOverride { direction, primary } => Ok(path_elements(vec![PatternElement::Relationship(simplified_edge(
            *direction,
            Some(simplified_label(primary)?),
        ))])),
        S::Alternatives { multiset, operands } => alternative_paths(
            *multiset,
            operands
                .iter()
                .map(|operand| simplified_adapter(operand, direction))
                .collect::<Lowered<Vec<_>>>()?,
        ),
        S::Concatenation(parts) => {
            let mut paths = parts.iter().map(|part| simplified_adapter(part, direction));
            let mut path = paths.next().ok_or(QueryFailureReason::Pattern)??;
            for next in paths {
                path = concatenate_paths(path, next?)?;
            }
            Ok(path)
        }
        S::Quantified { primary, length } => quantify_adapter(simplified_adapter(primary, direction)?, *length),
        _ => Err(QueryFailureReason::Pattern),
    }
}

fn simplified_label(expression: &hydradb_cypher_ast::SimplifiedPath) -> Lowered<LabelExpression> {
    use hydradb_cypher_ast::SimplifiedPath as S;
    let children = || expression.children().iter().map(simplified_label).collect::<Lowered<Vec<_>>>();
    Ok(match expression {
        S::Label(name) => LabelExpression::Label(name.clone()),
        S::Alternatives { multiset: false, .. } => LabelExpression::Or(children()?),
        S::Conjunction(_) => LabelExpression::And(children()?),
        S::Negated(primary) => LabelExpression::Not(Box::new(simplified_label(primary)?)),
        _ => return Err(QueryFailureReason::Pattern),
    })
}

fn path_elements(elements: Vec<PatternElement>) -> PathPattern {
    PathPattern {
        factor: None,
        binding: None,
        subpaths: Vec::new(),
        search: None,
        search_prefix_written: false,
        elements,
        modes: Vec::new(),
        predicate: None,
        alternatives: None,
    }
}

fn scoped_path(path: &PathPattern) -> bool {
    path.binding.is_some()
        || !path.subpaths.is_empty()
        || path.search_prefix_written
        || path.search.is_some()
        || !path.modes.is_empty()
        || path.predicate.is_some()
}

fn alternative_paths(multiset: bool, paths: Vec<PathPattern>) -> Lowered<PathPattern> {
    let mut operands = Vec::new();
    for mut path in paths {
        if path.alternatives.is_some() {
            if scoped_path(&path) {
                return Err(QueryFailureReason::Pattern);
            }
            let nested = path.alternatives.take().expect("checked alternatives");
            if nested.multiset != multiset {
                return Err(QueryFailureReason::Pattern);
            }
            operands.extend(nested.operands);
        } else {
            let predicate = path.predicate.take();
            operands.push(PathOperand { path, predicate });
        }
    }
    let mut path = path_elements(Vec::new());
    path.alternatives = Some(PathAlternatives { multiset, operands });
    Ok(path)
}

fn concatenate_paths(mut left: PathPattern, mut right: PathPattern) -> Lowered<PathPattern> {
    if scoped_path(&left) || scoped_path(&right) {
        return Err(QueryFailureReason::Pattern);
    }
    match (left.alternatives.take(), right.alternatives.take()) {
        (None, None) => {
            left.elements.extend(right.elements);
            Ok(left)
        }
        // Independent choice points need a structural executor, not an
        // exponentially expanded product of the flat operand adapter.
        (Some(_), Some(_)) => Err(QueryFailureReason::Pattern),
        (Some(alternatives), None) => {
            let paths = alternatives
                .operands
                .into_iter()
                .map(|operand| {
                    if operand.predicate.is_some() {
                        return Err(QueryFailureReason::Pattern);
                    }
                    concatenate_paths(operand.path, right.clone())
                })
                .collect::<Lowered<Vec<_>>>()?;
            alternative_paths(alternatives.multiset, paths)
        }
        (None, Some(alternatives)) => {
            let paths = alternatives
                .operands
                .into_iter()
                .map(|operand| {
                    if operand.predicate.is_some() {
                        return Err(QueryFailureReason::Pattern);
                    }
                    concatenate_paths(left.clone(), operand.path)
                })
                .collect::<Lowered<Vec<_>>>()?;
            alternative_paths(alternatives.multiset, paths)
        }
    }
}

fn quantify_adapter(mut path: PathPattern, length: PathLength) -> Lowered<PathPattern> {
    if scoped_path(&path) || path.alternatives.is_some() {
        return Err(QueryFailureReason::Pattern);
    }
    let [PatternElement::Relationship(edge)] = path.elements.as_mut_slice() else {
        return Err(QueryFailureReason::Pattern);
    };
    if edge.length.is_some() {
        return Err(QueryFailureReason::Pattern);
    }
    edge.length = Some(length);
    Ok(path)
}

fn finish_path(path: &mut PathPattern) -> Lowered<()> {
    if let Some(alternatives) = &mut path.alternatives {
        for operand in &mut alternatives.operands {
            finish_path(&mut operand.path)?;
        }
        return Ok(());
    }
    let mut elements = Vec::new();
    for element in std::mem::take(&mut path.elements) {
        if matches!(element, PatternElement::Relationship(_)) && !matches!(elements.last(), Some(PatternElement::Node(_))) {
            elements.push(PatternElement::Node(anonymous_node()));
        }
        if let PatternElement::Node(node) = &element {
            if let Some(PatternElement::Node(previous)) = elements.last() {
                if node == &anonymous_node() {
                    continue;
                }
                if previous == &anonymous_node() {
                    elements.pop();
                }
            }
        }
        elements.push(element);
    }
    if matches!(elements.last(), Some(PatternElement::Relationship(_))) {
        elements.push(PatternElement::Node(anonymous_node()));
    }
    if elements.is_empty() {
        return Err(QueryFailureReason::Pattern);
    }
    path.elements = elements;
    Ok(())
}

/// A node pattern that names, labels and tests nothing.
fn anonymous_node() -> NodePattern {
    NodePattern {
        variable: None,
        labels: Vec::new(),
        label_expression: None,
        properties: None,
        predicate: None,
    }
}

/// A decimal number's text without its exact number suffix `M`, underscores
/// removed, when it has the suffix and is otherwise a decimal (§21.2): each
/// run of digits may hold single underscores between its digits only, so
/// `1_M` and `1E_2M` are no literals.
fn exact_stem(text: &str) -> Option<String> {
    let normalized = hydradb_cypher_ast::numeric::decimal_spelling(text);
    let text = normalized.as_str();
    let stem = text.strip_suffix(['m', 'M'])?;
    let (mantissa, exponent) = match stem.find(['e', 'E']) {
        Some(index) => (&stem[..index], Some(&stem[index + 1..])),
        None => (stem, None),
    };
    let mantissa = match mantissa.split_once('.') {
        Some((whole, fraction)) if !(whole.is_empty() && fraction.is_empty()) => {
            let part = |part: &str| {
                if part.is_empty() {
                    Some(String::new())
                } else {
                    separated_digits(part)
                }
            };
            format!("{}.{}", part(whole)?, part(fraction)?)
        }
        Some(_) => return None,
        None => separated_digits(mantissa)?,
    };
    let exponent = match exponent {
        Some(exponent) => {
            let (sign, digits) = match exponent.strip_prefix(['+', '-']) {
                Some(digits) => (&exponent[..1], digits),
                None => ("", exponent),
            };
            format!("e{sign}{}", separated_digits(digits)?)
        }
        None => String::new(),
    };
    Some(format!("{mantissa}{exponent}"))
}

/// Decimal digits with single underscores between them, the underscores
/// removed (§21.2); `None` for anything else.
fn separated_digits(text: &str) -> Option<String> {
    hydradb_cypher_ast::numeric::separated_digits(text, 10)
}

/// A number's text without its approximate number suffix `F`, `f`, `D` or
/// `d` (§21.2), when it has one.
fn approximate_stem(text: &str) -> Option<&str> {
    text.strip_suffix(['f', 'F', 'd', 'D'])
}

/// Whether `text` spells an unsigned decimal integer, `_` separators allowed,
/// whatever its magnitude.
fn is_decimal_integer(text: &str) -> bool {
    separated_digits(text).is_some()
}

/// An unsigned decimal integer token, `_` separators allowed. A token the
/// lexer let carry a letter suffix is not a number.
fn digits(text: &str) -> Option<i128> {
    let cleaned = numeric_spelling(text);
    if cleaned.is_empty() || !cleaned.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    cleaned.parse().ok()
}

fn numeric_spelling(text: &str) -> String {
    hydradb_cypher_ast::numeric::decimal_spelling(text).replace('_', "")
}

fn unsigned_integer(kind: i32, text: &str) -> Option<i128> {
    match kind {
        grammar::UNSIGNED_DECIMAL_INTEGER => digits(text),
        grammar::UNSIGNED_HEX_INTEGER => radix(text, "0x", 16),
        grammar::UNSIGNED_OCTAL_INTEGER => radix(text, "0o", 8),
        grammar::UNSIGNED_BINARY_INTEGER => radix(text, "0b", 2),
        _ => None,
    }
}

fn radix(text: &str, prefix: &str, radix: u32) -> Option<i128> {
    let cleaned: String = text.strip_prefix(prefix)?.chars().filter(|character| *character != '_').collect();
    if cleaned.is_empty() {
        return None;
    }
    i128::from_str_radix(&cleaned, radix).ok()
}

/// The spelling a float literal keeps. A plain `digits.digits` spelling is
/// kept as written so no decimal precision is lost before typing; any other
/// form (`.5`, `1e3`, `1_000.5`) is rewritten to that plain spelling.
fn float_spelling(text: &str) -> Option<String> {
    let cleaned = numeric_spelling(text);
    if !cleaned
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'.' | b'e' | b'E' | b'+' | b'-'))
    {
        return None;
    }
    let plain = cleaned.split_once('.').is_some_and(|(whole, fraction)| {
        !whole.is_empty() && !fraction.is_empty() && (whole.bytes().chain(fraction.bytes())).all(|byte| byte.is_ascii_digit())
    });
    if plain {
        return Some(cleaned);
    }
    let value: f64 = cleaned.parse().ok()?;
    // Out of FLOAT's range: keep the spelling, so the engine can report the
    // literal (§21.2 Syntax Rule 20) rather than an untyped query.
    Some(if value.is_finite() { format!("{value:?}") } else { cleaned })
}
