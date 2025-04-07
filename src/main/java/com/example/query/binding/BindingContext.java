package com.example.query.binding;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Represents a context for variable bindings during query execution.
 * Tracks the values bound to variables and provides methods to access them.
 */
public class BindingContext {
    // Map of variable name to list of bound values
    private final Map<String, List<Object>> bindings;
    
    /**
     * Creates an empty binding context.
     */
    public BindingContext() {
        this.bindings = new ConcurrentHashMap<>();
    }
    
    /**
     * Creates a binding context with initial bindings.
     *
     * @param initialBindings The initial bindings to populate the context with
     */
    public BindingContext(Map<String, ?> initialBindings) {
        this.bindings = new ConcurrentHashMap<>();
        
        if (initialBindings != null) {
            initialBindings.forEach((name, value) -> {
                String formattedName = Variable.formatName(name);
                if (value instanceof Collection<?> collection) {
                    bindValues(formattedName, collection);
                } else {
                    bindValue(formattedName, value);
                }
            });
        }
    }
    
    /**
     * Creates a binding context by copying another context.
     *
     * @param other The context to copy
     */
    private BindingContext(BindingContext other) {
        this.bindings = new ConcurrentHashMap<>();
        
        // Deep copy all bindings
        other.bindings.forEach((name, values) -> {
            List<Object> valuesCopy = new CopyOnWriteArrayList<>(values);
            this.bindings.put(name, valuesCopy);
        });
    }
    
    /**
     * Binds a value to a variable.
     *
     * @param <T> The type of the value
     * @param variableName The variable name
     * @param value The value to bind
     */
    public <T> void bindValue(String variableName, T value) {
        if (value == null) {
            return;
        }
        
        String formattedName = Variable.formatName(variableName);
        bindings.computeIfAbsent(formattedName, k -> new CopyOnWriteArrayList<>())
                .add(value);
    }
    
    /**
     * Binds multiple values to a variable.
     *
     * @param <T> The type of the values
     * @param variableName The variable name
     * @param values The values to bind
     */
    public <T> void bindValues(String variableName, Collection<T> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        
        String formattedName = Variable.formatName(variableName);
        List<Object> bindingsList = bindings.computeIfAbsent(formattedName, k -> new CopyOnWriteArrayList<>());
        
        // Add all values, filtering out nulls
        values.stream()
              .filter(v -> v != null)
              .forEach(bindingsList::add);
    }
    
    /**
     * Gets a value bound to a variable.
     *
     * @param <T> The expected type of the value
     * @param variableName The variable name
     * @param type The class representing the expected type
     * @return Optional containing the value if found and of the expected type, empty otherwise
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getValue(String variableName, Class<T> type) {
        String formattedName = Variable.formatName(variableName);
        List<Object> values = bindings.get(formattedName);
        
        if (values == null || values.isEmpty()) {
            return Optional.empty();
        }
        
        // Find the first value of the expected type
        return values.stream()
            .filter(v -> type.isInstance(v))
            .map(v -> (T) v)
            .findFirst();
    }
    
    /**
     * Gets all values bound to a variable.
     *
     * @param <T> The expected type of the values
     * @param variableName The variable name
     * @param type The class representing the expected type
     * @return List of values bound to the variable of the expected type
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getValues(String variableName, Class<T> type) {
        String formattedName = Variable.formatName(variableName);
        List<Object> values = bindings.get(formattedName);
        
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Filter values by type and cast
        return values.stream()
            .filter(v -> type.isInstance(v))
            .map(v -> (T) v)
            .collect(Collectors.toList());
    }
    
    /**
     * Checks if a variable has at least one bound value.
     *
     * @param variableName The variable name
     * @return true if the variable has at least one bound value, false otherwise
     */
    public boolean hasValue(String variableName) {
        String formattedName = Variable.formatName(variableName);
        List<Object> values = bindings.get(formattedName);
        return values != null && !values.isEmpty();
    }
    
    /**
     * Gets all variables in this context.
     *
     * @return Set of all variable names
     */
    public Set<String> getVariableNames() {
        return Collections.unmodifiableSet(bindings.keySet());
    }
    
    /**
     * Creates a new binding context with the same bindings.
     *
     * @return A new binding context with the same bindings
     */
    public BindingContext copy() {
        return new BindingContext(this);
    }
    
    /**
     * Merges this binding context with another, creating a new context.
     * Values from the other context take precedence in case of conflicts.
     *
     * @param other The other binding context to merge with
     * @return A new binding context with merged bindings
     */
    public BindingContext merge(BindingContext other) {
        BindingContext result = new BindingContext(this);
        
        // Add all bindings from the other context
        other.getVariableNames().forEach(name -> {
            // Get all values of any type from the other context
            List<Object> values = other.bindings.getOrDefault(name, Collections.emptyList());
            result.bindValues(name, values);
        });
        
        return result;
    }
    
    /**
     * Creates an empty binding context.
     *
     * @return A new empty binding context
     */
    public static BindingContext empty() {
        return new BindingContext();
    }
    
    /**
     * Creates a binding context from a map of variable names to values.
     *
     * @param bindings The map of variable names to values
     * @return A new binding context with the provided bindings
     */
    public static BindingContext of(Map<String, ?> bindings) {
        return new BindingContext(bindings);
    }
    
    @Override
    public String toString() {
        return "BindingContext{bindings=" + bindings + "}";
    }
} 