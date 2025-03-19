package com.example.query.binding;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Registry for all variables in a query.
 * Tracks both producer and consumer variables.
 */
public class VariableRegistry {
    // Map of variable name to producers
    private final Map<String, Set<ProducerVariable>> producers = new ConcurrentHashMap<>();
    
    // Map of variable name to consumers
    private final Map<String, Set<ConsumerVariable>> consumers = new ConcurrentHashMap<>();
    
    /**
     * Registers a producer variable.
     *
     * @param name The variable name
     * @param type The variable type
     * @param conditionType The condition type that produces the variable
     * @return The registered producer variable
     */
    public ProducerVariable registerProducer(String name, VariableType type, String conditionType) {
        String formattedName = Variable.formatName(name);
        ProducerVariable var = new ProducerVariable(formattedName, type, conditionType);
        
        producers.computeIfAbsent(formattedName, k -> ConcurrentHashMap.newKeySet())
                .add(var);
                
        return var;
    }
    
    /**
     * Registers a consumer variable.
     *
     * @param name The variable name
     * @param type The variable type
     * @param conditionType The condition type that consumes the variable
     * @return The registered consumer variable
     */
    public ConsumerVariable registerConsumer(String name, VariableType type, String conditionType) {
        String formattedName = Variable.formatName(name);
        ConsumerVariable var = new ConsumerVariable(formattedName, type, conditionType);
        
        consumers.computeIfAbsent(formattedName, k -> ConcurrentHashMap.newKeySet())
                .add(var);
                
        return var;
    }
    
    /**
     * Gets all producer variables for a given name.
     *
     * @param name The variable name
     * @return Unmodifiable set of producer variables
     */
    public Set<ProducerVariable> getProducers(String name) {
        String formattedName = Variable.formatName(name);
        return Collections.unmodifiableSet(
            producers.getOrDefault(formattedName, Collections.emptySet())
        );
    }
    
    /**
     * Gets all consumer variables for a given name.
     *
     * @param name The variable name
     * @return Unmodifiable set of consumer variables
     */
    public Set<ConsumerVariable> getConsumers(String name) {
        String formattedName = Variable.formatName(name);
        return Collections.unmodifiableSet(
            consumers.getOrDefault(formattedName, Collections.emptySet())
        );
    }
    
    /**
     * Checks if a variable is produced (has at least one producer).
     *
     * @param name The variable name
     * @return true if the variable is produced, false otherwise
     */
    public boolean isProduced(String name) {
        String formattedName = Variable.formatName(name);
        return producers.containsKey(formattedName) && !producers.get(formattedName).isEmpty();
    }
    
    /**
     * Gets all variable names in the registry.
     *
     * @return Unmodifiable set of all variable names
     */
    public Set<String> getAllVariableNames() {
        Set<String> allNames = new HashSet<>();
        allNames.addAll(producers.keySet());
        allNames.addAll(consumers.keySet());
        return Collections.unmodifiableSet(allNames);
    }
    
    /**
     * Gets all producer variables in the registry.
     *
     * @return Unmodifiable collection of all producer variables
     */
    public Collection<ProducerVariable> getAllProducers() {
        return producers.values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.toUnmodifiableSet());
    }
    
    /**
     * Gets all consumer variables in the registry.
     *
     * @return Unmodifiable collection of all consumer variables
     */
    public Collection<ConsumerVariable> getAllConsumers() {
        return consumers.values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.toUnmodifiableSet());
    }
    
    /**
     * Gets the inferred type for a variable, based on all its producers and consumers.
     * If there are conflicting types, ANY is returned.
     *
     * @param name The variable name
     * @return The inferred type, or ANY if unknown or conflicting
     */
    public VariableType getInferredType(String name) {
        String formattedName = Variable.formatName(name);
        
        // Collect all types from producers
        Set<VariableType> producerTypes = producers.getOrDefault(formattedName, Collections.emptySet())
            .stream()
            .map(Variable::getType)
            .collect(Collectors.toSet());
            
        // Collect all types from consumers
        Set<VariableType> consumerTypes = consumers.getOrDefault(formattedName, Collections.emptySet())
            .stream()
            .map(Variable::getType)
            .collect(Collectors.toSet());
            
        // Combine all types
        Set<VariableType> allTypes = new HashSet<>();
        allTypes.addAll(producerTypes);
        allTypes.addAll(consumerTypes);
        
        // No types, return ANY
        if (allTypes.isEmpty()) {
            return VariableType.ANY;
        }
        
        // If there's exactly one type, return it
        if (allTypes.size() == 1) {
            return allTypes.iterator().next();
        }
        
        // If ANY is one of the types, remove it and check again
        if (allTypes.contains(VariableType.ANY)) {
            allTypes.remove(VariableType.ANY);
            if (allTypes.size() == 1) {
                return allTypes.iterator().next();
            }
        }
        
        // Multiple specific types means there's a conflict
        return VariableType.ANY;
    }
    
    /**
     * Validates that all consumer variables have corresponding producers.
     *
     * @return Set of validation error messages, empty if valid
     */
    public Set<String> validate() {
        Set<String> errors = new HashSet<>();
        
        // Check that all consumed variables are produced
        for (String name : consumers.keySet()) {
            if (!isProduced(name)) {
                errors.add("Variable " + name + " is consumed but never produced");
            }
        }
        
        return errors;
    }
    
    /**
     * Clears all variables from the registry.
     */
    public void clear() {
        producers.clear();
        consumers.clear();
    }
} 