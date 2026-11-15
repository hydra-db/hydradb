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
