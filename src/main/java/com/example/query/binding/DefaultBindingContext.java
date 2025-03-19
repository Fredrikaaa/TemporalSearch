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
 * Default implementation of BindingContext.
 * Uses thread-safe collections for concurrent access.
 */
public class DefaultBindingContext implements BindingContext {
    // Map of variable name to list of bound values
    private final Map<String, List<Object>> bindings;
    
    /**
     * Creates an empty binding context.
     */
    public DefaultBindingContext() {
        this.bindings = new ConcurrentHashMap<>();
    }
    
    /**
     * Creates a binding context with initial bindings.
     *
     * @param initialBindings The initial bindings to populate the context with
     */
    public DefaultBindingContext(Map<String, ?> initialBindings) {
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
    private DefaultBindingContext(DefaultBindingContext other) {
        this.bindings = new ConcurrentHashMap<>();
        
        // Deep copy all bindings
        other.bindings.forEach((name, values) -> {
            List<Object> valuesCopy = new CopyOnWriteArrayList<>(values);
            this.bindings.put(name, valuesCopy);
        });
    }
    
    @Override
    public <T> void bindValue(String variableName, T value) {
        if (value == null) {
            return;
        }
        
        String formattedName = Variable.formatName(variableName);
        bindings.computeIfAbsent(formattedName, k -> new CopyOnWriteArrayList<>())
                .add(value);
    }
    
    @Override
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
    
    @SuppressWarnings("unchecked")
    @Override
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
    
    @SuppressWarnings("unchecked")
    @Override
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
    
    @Override
    public boolean hasValue(String variableName) {
        String formattedName = Variable.formatName(variableName);
        List<Object> values = bindings.get(formattedName);
        return values != null && !values.isEmpty();
    }
    
    @Override
    public Set<String> getVariableNames() {
        return Collections.unmodifiableSet(bindings.keySet());
    }
    
    @Override
    public BindingContext copy() {
        return new DefaultBindingContext(this);
    }
    
    @Override
    public BindingContext merge(BindingContext other) {
        DefaultBindingContext result = new DefaultBindingContext(this);
        
        // Add all bindings from the other context
        other.getVariableNames().forEach(name -> {
            // Get all values of any type from the other context
            // We can't use the getValues method with a specific type
            // Instead, iterate over the variable names and access directly
            List<Object> values = ((DefaultBindingContext) other).bindings.getOrDefault(name, Collections.emptyList());
            result.bindValues(name, values);
        });
        
        return result;
    }
    
    @Override
    public String toString() {
        return "BindingContext{bindings=" + bindings + "}";
    }
} 