README
Join implementation
Join implementation is split between two files: JoinOperator and CustomJoinExpressionParser. JoinOperator simply implements the algorithm given in the assignment spec: it scans the outer child once, and then for each tuple in the outer child it scans the inner child completely. While doing that, it uses a CustomExprerssionParser to check if each tuple satisfies the given expression.

The second class, CustomJoinExpressionParser, generates a left deep-join tree of operators. To do this, it uses a new method parseBinaryExpression to parse any relational binary expressions (=, !=, >, <, etc.). This method checks if the expression is a join expression (which consists of two columns from different tables) or a select expression (which covers everything else, like "Sailors.A > 1" and "1 = 1"). If it's a join expression, then we calculate a merge index - which is the maximum of indices of the two columns in the FROM clause. For example, for SELECT * FROM a, b, c, merge_index(c, a) is 2 (c is the first item in this list, and we start from 0). We then insert the expression in a list of join expressions. If there is another expression already there, we simply join them using an AND operator.

Select expressions are all other expressions. For these, we work out which side has the column reference, and add the expression to a select list. Again, we use a merge index to determine where the expression should be added and we use AND if there is already an expression there. If the expression does not contain column references, we add it at index 0 - this allows us to evaluate expressions like 1 = 0 early.

Once parsing is done, we assemble the tree: for each table, we create a ScanOperator, add a SelectOperator with the combined select expression, and finally merge the current root with the new child. We repeat this until we reach the last merge index.

This implementation has O(parsing) + O(n) complexity, where n is the number of tables we're merging.

Example:
SELECT * FROM Sailors, Boats, Reservations WHERE Sailors.A = Boats.B AND Sailors.A > Boats.C AND Sailors.A > 0 AND Reservations.D > Sailors.A AND 1 > 0 AND Reservations.F < 42

Select expressions: ["Sailors.A > 0 AND 1 > 0", "", "Reservations.F < 42"]

Join expressions: ["", "Sailors.A = Boats.B AND Sailors.A > Boats.C", "Reservations.D > Sailors.A"]

This produces a query plan similar to the one in the coursework specification.

Bugs
There are no know bugs in this project (apart from the ones that are not in the scope of this project).