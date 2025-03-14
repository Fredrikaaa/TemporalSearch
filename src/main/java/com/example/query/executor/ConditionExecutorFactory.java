package com.example.query.executor;

import com.example.query.model.condition.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for condition executors using pattern matching and singleton instances.
 * Maintains type safety through sealed interfaces.
 */
public final class ConditionExecutorFactory {
    private static final Logger logger = LoggerFactory.getLogger(ConditionExecutorFactory.class);
    
    // Logical and Not executors are singletons since they don't need variable names
    private final LogicalExecutor logicalExecutor;
    private final NotExecutor notExecutor;
    
    /**
     * Creates a new ConditionExecutorFactory with singleton executor instances.
     */
    public ConditionExecutorFactory() {
        // Logical and Not executors need this factory for recursive execution
        this.logicalExecutor = new LogicalExecutor(this);
        this.notExecutor = new NotExecutor(this);
        
        logger.debug("Initialized condition executor factory");
    }
    
    /**
     * Gets the appropriate executor for a condition using pattern matching.
     * For conditions that support variable binding, creates a new executor instance
     * with the variable name from the condition.
     *
     * @param <T> The condition type
     * @param condition The condition
     * @return The executor for the condition
     * @throws IllegalArgumentException if the condition type is not supported
     */
    @SuppressWarnings("unchecked")
    public <T extends Condition> ConditionExecutor<T> getExecutor(T condition) {
        logger.debug("Getting executor for condition type: {}", condition.getType());
        
        return (ConditionExecutor<T>) switch (condition) {
            case Contains c -> new ContainsExecutor(c.variableName());
            case Ner c -> new NerExecutor(c.variableName());
            case Pos c -> new PosExecutor(c.variableName());
            case Dependency c -> new DependencyExecutor(c.variableName());
            case Logical c -> logicalExecutor;
            case Not c -> notExecutor;
            case Temporal c -> throw new UnsupportedOperationException(
                "Temporal condition execution not yet implemented");
        };
    }
} 