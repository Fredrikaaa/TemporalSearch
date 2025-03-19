package com.example.query.executor;

import com.example.core.IndexAccess;
import com.example.query.model.DocSentenceMatch;
import com.example.query.model.Query;
import com.example.query.model.condition.Temporal;
import com.example.query.binding.BindingContext;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor for temporal conditions.
 * Currently a stub implementation that throws UnsupportedOperationException.
 */
public final class TemporalExecutor implements ConditionExecutor<Temporal> {
    private static final Logger logger = LoggerFactory.getLogger(TemporalExecutor.class);

    @Override
    public Set<DocSentenceMatch> execute(
            Temporal condition,
            Map<String, IndexAccess> indexes,
            BindingContext bindingContext,
            Query.Granularity granularity,
            int granularitySize) throws QueryExecutionException {
        
        logger.debug("Temporal condition execution requested but not yet implemented");
        throw new UnsupportedOperationException("TemporalCondition execution not yet implemented");
    }
} 