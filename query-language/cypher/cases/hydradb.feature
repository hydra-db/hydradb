Feature: HydraDB local Cypher contracts

  Scenario: Preserve graph identity as a scalar through both production adapters
    Given an empty graph
    And having executed:
      """
      CREATE (a:Value {id: 1})-[:LINK]->(b:Other {id: 2})
      """
    When executing query:
      """
      MATCH (n:Value {id: 1}) RETURN n.id AS value
      """
    Then the result should be, in any order:
      | value |
      | 1 |
    And no side effects

  Scenario: Return a string prefix predicate through both production adapters
    Given an empty graph
    And having executed:
      """
      CREATE (a:Value {id: 1, name: 'hydradb'})-[:LINK]->(b:Other {id: 2})
      """
    When executing query:
      """
      MATCH (n:Value) WHERE n.name STARTS WITH 'hydra' RETURN n.id AS value
      """
    Then the result should be, in any order:
      | value |
      | 1 |
    And no side effects

  Scenario: Observe a committed shared transactional mutation
    Given an empty graph
    When executing query:
      """
      CREATE (a:A {id: 1})-[:LINK]->(b:B {id: 2})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
      | +labels        | 2 |
    When executing control query:
      """
      MATCH (a:A)-[:LINK]->(b:B)
      RETURN a.id AS source, b.id AS target
      """
    Then the result should be, in any order:
      | source | target |
      | 1      | 2      |

  Scenario: EXPLAIN of CREATE is refused and writes nothing
    Given an empty graph
    When executing query:
      """
      EXPLAIN CREATE (a:A {id: 1})-[:LINK]->(b:B {id: 2})
      """
    Then an UnsupportedQuery should be raised at any time: *
    When executing control query:
      """
      MATCH (a:A)-[:LINK]->(b:B) RETURN a.id AS source
      """
    Then the result should be empty

  Scenario: Lowercase explain of MERGE is refused and writes nothing
    Given an empty graph
    When executing query:
      """
      explain MERGE (a:A {id: 1})-[:LINK]->(b:B {id: 2})
      """
    Then an UnsupportedQuery should be raised at any time: *
    When executing control query:
      """
      MATCH (a:A)-[:LINK]->(b:B) RETURN a.id AS source
      """
    Then the result should be empty

  Scenario: EXPLAIN of SET is refused and leaves the property unchanged
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {id: 1, name: 'original'})-[:LINK]->(b:B {id: 2})
      """
    When executing query:
      """
      EXPLAIN MATCH (a:A {id: 1}) SET a.name = 'changed'
      """
    Then an UnsupportedQuery should be raised at any time: *
    When executing control query:
      """
      MATCH (a:A {id: 1}) RETURN a.name AS name
      """
    Then the result should be, in any order:
      | name       |
      | 'original' |

  Scenario: EXPLAIN of DETACH DELETE is refused and keeps the node
    Given an empty graph
    And having executed:
      """
      CREATE (a:A {id: 1})-[:LINK]->(b:B {id: 2})
      """
    When executing query:
      """
      EXPLAIN MATCH (a:A {id: 1}) DETACH DELETE a
      """
    Then an UnsupportedQuery should be raised at any time: *
    When executing control query:
      """
      MATCH (a:A)-[:LINK]->(b:B) RETURN a.id AS source, b.id AS target
      """
    Then the result should be, in any order:
      | source | target |
      | 1      | 2      |
