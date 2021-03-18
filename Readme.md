# README
## Join implementation
Join implementation is split between two files: `JoinOperator` and `CustomJoinExpressionParser`. 
`JoinOperator` simply implements the algorithm given in the assignment spec: it scans the outer child once, 
and then for each tuple in the outer child it scans the inner child completely. While doing that, it uses a `CustomExprerssionParser` 
to check if each tuple satisfies the given expression.

The second class, `CustomJoinExpressionParser`, generates a left deep-join tree of operators. To do this, 
it uses a new method `parseBinaryExpression` to parse any relational binary expressions (=, !=, >, <, etc.).
This method checks if the expression is a _join expression_ (which consists of two columns from different tables) or
a _select expression_ (which covers everything else, like "Sailors.A > 1" and "1 = 1"). If it's a _join expression_,
then we calculate a _merge index_ - which is the maximum of indices of the two columns in the __FROM__ clause.
For example, for __SELECT * FROM a, b, c__, _merge_index(c, a)_ is 2 (c is the first item in this list, 
and we start from 0). We then insert the expression in a list of join expressions. If there is another expression
already there, we simply join them using an __AND__ operator.

_Select expressions_ are all other expressions. For these, we work out which side has the column reference, and add 
the expression to a _select list_. Again, we use a merge index to determine where the expression should be added and we 
use __AND__ if there is already an expression there. If the expression does not contain column references, we add it at
index 0 - this allows us to evaluate expressions like __1 = 0__ early.

Once parsing is done, we assemble the tree: for each table, we create a `ScanOperator`, add a `SelectOperator` with the 
combined _select expression_, and finally merge the current root with the new child. We repeat this until we reach the 
last _merge index_.

This implementation has O(parsing) + O(n) complexity, where n is the number of tables we're merging.

###Example:
__SELECT * FROM Sailors, Boats, Reservations WHERE Sailors.A = Boats.B AND Sailors.A > Boats.C AND Sailors.A > 0 AND Reservations.D > Sailors.A AND 1 > 0 AND Reservations.F < 42__

Select expressions: 
__["Sailors.A > 0 AND 1 > 0", "", "Reservations.F < 42"]__

Join expressions: 
__["", "Sailors.A = Boats.B AND Sailors.A > Boats.C", "Reservations.D > Sailors.A"]__

This produces a query plan similar to the one in the coursework specification.

## Bugs
There are no know bugs in this project (apart from the ones that are not in the scope of this project).