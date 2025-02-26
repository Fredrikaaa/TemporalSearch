package com.example.query.executor;

import com.example.query.model.Condition;
import com.example.query.model.ContainsCondition;
import com.example.query.model.DependencyCondition;
import com.example.query.model.LogicalCondition;
import com.example.query.model.NerCondition;
import com.example.query.model.NotCondition;
import com.example.query.model.TemporalCondition;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating condition executors based on condition type.
 * Maintains a registry of executors for different condition types.
 */
public class ConditionExecutorFactory {
    private static final Logger logger = LoggerFactory.getLogger(ConditionExecutorFactory.class);
    
    private final Map<Class<? extends Condition>, ConditionExecutor<?>> executors;

    /**
     * Creates a new ConditionExecutorFactory with default executors.
     */
    public ConditionExecutorFactory() {
        this.executors = new HashMap<>();
        registerDefaultExecutors();
    }

    /**
     * Registers the default set of condition executors.
     */
    private void registerDefaultExecutors() {
        // These will be implemented later
        // For now, we just register the classes to establish the structure
        logger.debug("Registering default condition executors");
        
        // TODO: Implement these executor classes
        // executors.put(ContainsCondition.class, new ContainsConditionExecutor());
        // executors.put(NerCondition.class, new NerConditionExecutor());
        // executors.put(TemporalCondition.class, new TemporalConditionExecutor());
        // executors.put(DependencyCondition.class, new DependencyConditionExecutor());
        // executors.put(LogicalCondition.class, new LogicalConditionExecutor(this));
        // executors.put(NotCondition.class, new NotConditionExecutor(this));
    }

    /**
     * Registers a condition executor for a specific condition type.
     *
     * @param <T> The condition type
     * @param conditionClass The condition class
     * @param executor The executor for the condition
     */
    public <T extends Condition> void registerExecutor(Class<T> conditionClass, ConditionExecutor<T> executor) {
        executors.put(conditionClass, executor);
        logger.debug("Registered executor for condition type: {}", conditionClass.getSimpleName());
    }

    /**
     * Gets the appropriate executor for a condition.
     *
     * @param <T> The condition type
     * @param condition The condition
     * @return The executor for the condition
     * @throws IllegalArgumentException if no executor is found for the condition type
     */
    @SuppressWarnings("unchecked")
    public <T extends Condition> ConditionExecutor<T> getExecutor(T condition) {
        ConditionExecutor<?> executor = executors.get(condition.getClass());
        if (executor == null) {
            String message = "No executor found for condition type: " + condition.getClass().getName();
            logger.error(message);
            throw new IllegalArgumentException(message);
        }
        
        logger.debug("Found executor for condition type: {}", condition.getClass().getSimpleName());
        return (ConditionExecutor<T>) executor;
    }
} 