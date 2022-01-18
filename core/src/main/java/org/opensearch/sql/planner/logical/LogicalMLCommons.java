package org.opensearch.sql.planner.logical;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Argument;

import java.util.Collections;
import java.util.List;

/**
 * ml-commons logical plan
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalMLCommons extends LogicalPlan {

    private final String algorithm;

    private final List<Argument> arguments;

    private final String modelId;
    /**
     * Constructor of LogicalMLCommons
     * @param child child logical plan
     * @param algorithm algorithm name
     * @param arguments arguments of the algorithm
     */
    public LogicalMLCommons(LogicalPlan child,
                            String algorithm,
                            List<Argument> arguments) {
        this(child, algorithm, arguments, null);
    }

    /**
     * Constructor of LogicalMLCommons.
     * @param child child logical plan
     * @param algorithm algorithm name
     * @param arguments arguments of the algorithm
     * @param modelId id of model
     */
    public LogicalMLCommons(LogicalPlan child,
                            String algorithm,
                            List<Argument> arguments,
                            String modelId) {
        super(Collections.singletonList(child));
        this.algorithm = algorithm;
        this.arguments = arguments;
        this.modelId = modelId;
    }

    @Override
    public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitMLCommons(this, context);
    }
}
