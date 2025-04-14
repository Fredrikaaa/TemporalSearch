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
    
    // Executors are now all singletons since they don't need variable names
    private final LogicalExecutor logicalExecutor;
    private final NerExecutor nerExecutor;
    private final ContainsExecutor containsExecutor;
    private final PosExecutor posExecutor;
    private final DependencyExecutor dependencyExecutor;
    private final TemporalExecutor temporalExecutor;
    private final NotExecutor notExecutor;
    
    /**
     * Creates a new ConditionExecutorFactory with singleton executor instances.
     */
    public ConditionExecutorFactory() {
        // Logical executor needs this factory for recursive execution
        this.logicalExecutor = new LogicalExecutor(this);
        
        // Other executors are now also singletons
        this.nerExecutor = new NerExecutor();
        this.containsExecutor = new ContainsExecutor();
        this.posExecutor = new PosExecutor();
        this.dependencyExecutor = new DependencyExecutor();
        this.temporalExecutor = new TemporalExecutor();
        this.notExecutor = new NotExecutor(this);
        
        logger.debug("Initialized condition executor factory");
    }
    
    /**
     * Gets the appropriate executor for a condition using pattern matching.
     * All executors are now singletons since variable binding is done through
     * the condition itself and the binding context.
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
            case Contains c -> containsExecutor;
            case Ner c -> nerExecutor;
            case Pos c -> posExecutor;
            case Dependency c -> dependencyExecutor;
            case Logical c -> logicalExecutor;
            case Temporal c -> temporalExecutor;
            case Not c -> notExecutor;
            default -> throw new IllegalArgumentException("Unsupported condition type: " + condition.getClass().getSimpleName());
        };
    }
} 