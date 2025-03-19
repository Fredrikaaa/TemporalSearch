package com.example.query.binding;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a context for variable bindings during query execution.
 * Tracks the values bound to variables and provides methods to access them.
 */
public interface BindingContext {
    
    /**
     * Binds a value to a variable.
     *
     * @param <T> The type of the value
     * @param variableName The variable name
     * @param value The value to bind
     */
    <T> void bindValue(String variableName, T value);
    
    /**
     * Binds multiple values to a variable.
     *
     * @param <T> The type of the values
     * @param variableName The variable name
     * @param values The values to bind
     */
    <T> void bindValues(String variableName, Collection<T> values);
    
    /**
     * Gets a value bound to a variable.
     *
     * @param <T> The expected type of the value
     * @param variableName The variable name
     * @param type The class representing the expected type
     * @return Optional containing the value if found and of the expected type, empty otherwise
     */
    <T> Optional<T> getValue(String variableName, Class<T> type);
    
    /**
     * Gets all values bound to a variable.
     *
     * @param <T> The expected type of the values
     * @param variableName The variable name
     * @param type The class representing the expected type
     * @return List of values bound to the variable of the expected type
     */
    <T> List<T> getValues(String variableName, Class<T> type);
    
    /**
     * Checks if a variable has at least one bound value.
     *
     * @param variableName The variable name
     * @return true if the variable has at least one bound value, false otherwise
     */
    boolean hasValue(String variableName);
    
    /**
     * Gets all variables in this context.
     *
     * @return Set of all variable names
     */
    Set<String> getVariableNames();
    
    /**
     * Creates a new binding context with the same bindings.
     *
     * @return A new binding context with the same bindings
     */
    BindingContext copy();
    
    /**
     * Merges this binding context with another, creating a new context.
     * Values from the other context take precedence in case of conflicts.
     *
     * @param other The other binding context to merge with
     * @return A new binding context with merged bindings
     */
    BindingContext merge(BindingContext other);
    
    /**
     * Creates an empty binding context.
     *
     * @return A new empty binding context
     */
    static BindingContext empty() {
        return new DefaultBindingContext();
    }
    
    /**
     * Creates a binding context from a map of variable names to values.
     *
     * @param bindings The map of variable names to values
     * @return A new binding context with the provided bindings
     */
    static BindingContext of(Map<String, ?> bindings) {
        return new DefaultBindingContext(bindings);
    }
} 