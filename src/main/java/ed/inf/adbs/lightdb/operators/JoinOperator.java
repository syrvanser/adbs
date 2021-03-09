package ed.inf.adbs.lightdb.operators;

import ed.inf.adbs.lightdb.CustomExpressionParser;
import ed.inf.adbs.lightdb.Tuple;
import net.sf.jsqlparser.expression.Expression;

import java.util.stream.Collectors;
import java.util.stream.Stream;


public class JoinOperator extends Operator{
    final protected Operator leftChild, rightChild;
    final protected Expression expression;
    protected Tuple currentLeftTuple;

    public JoinOperator (Operator leftChild, Operator rightChild, Expression expression){
        super(leftChild.getTableName() + "_join_" + rightChild.getTableName());
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.expression = expression;
        databaseCatalog.addTableJoin(leftChild.getTableName(), rightChild.getTableName());
        currentLeftTuple = leftChild.getNextTuple();
    }

    @Override
    public Tuple getNextTuple() {
        Tuple currentRightTuple;
        while(currentLeftTuple != null){
            while((currentRightTuple = rightChild.getNextTuple()) != null){
                Tuple mergedTuple = new Tuple(Stream.concat(currentLeftTuple.toList().stream(), currentRightTuple.toList().stream())
                        .collect(Collectors.toList()));
                CustomExpressionParser parser = new CustomExpressionParser(mergedTuple, getTableName());
                if (expression == null){
                    return mergedTuple;
                }
//                System.out.println(expression);
//                System.out.println(mergedTuple);
                expression.accept(parser);

                if (parser.getResult()){
                    return mergedTuple;
                }
            }
            rightChild.reset();
            currentLeftTuple = leftChild.getNextTuple();
        }
        return null;

    }

    @Override
    public void reset() {
        leftChild.reset();
        rightChild.reset();
    }
}
