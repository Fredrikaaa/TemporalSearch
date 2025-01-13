# Java NLP Pipeline

A high-performance natural language processing pipeline that performs text annotation and n-gram indexing with advanced logging and analysis capabilities.

## Features

- **Text Annotation**: Uses Stanford CoreNLP for robust text analysis
- **N-gram Indexing**: Generates unigram, bigram, and trigram indexes
- **Parallel Processing**: Utilizes multi-threading for improved performance
- **Flexible Storage**: Uses SQLite for document storage and LevelDB for index storage
- **Advanced Logging**: Comprehensive logging with performance metrics and error tracking
- **Analysis Tools**: Built-in tools for analyzing processing metrics and performance

## Prerequisites

- Java 17 or higher
- Maven 3.6 or higher
- At least 4GB of RAM (8GB recommended for large datasets)
- Sufficient disk space for indexes (depends on data size)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/java-nlp.git
   cd java-nlp
   ```

2. Build the project:
   ```bash
   mvn clean package
   ```

This will create an executable JAR file in the `target` directory.

## Usage

The pipeline consists of three main stages:

1. **Annotation**: Processes text using Stanford CoreNLP
2. **Indexing**: Generates n-gram indexes
3. **Analysis**: Analyzes processing logs for performance metrics and error patterns

### Basic Usage

Run all processing stages (annotation and indexing):

```bash
java -jar target/java-nlp-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --stage all \
    --db data.db \
    --index-dir indexes
```

### Stage-Specific Usage

Run only the annotation stage:

```bash
java -jar target/java-nlp-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --stage annotate \
    --db data.db \
    --batch_size 1000 \
    --threads 8
```

Run only the indexing stage:

```bash
java -jar target/java-nlp-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --stage index \
    --db data.db \
    --index-dir indexes \
    --index-type all
```

Analyze processing logs:

```bash
java -jar target/java-nlp-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --stage analyze \
    --log-file logs/indexer.log \
    --report-format both
```

### Command-Line Arguments

Common Arguments:

- `--stage`: Pipeline stage to run (`all`, `annotate`, `index`, or `analyze`)
- `--db`: SQLite database file path (required for annotation and indexing)
- `--batch_size`: Batch size for processing (default: 1000)

Annotation-Specific:

- `--threads`: Number of CoreNLP threads (default: 8)
- `--limit`: Limit the number of documents to process

Indexing-Specific:

- `--index-dir`: Directory for storing indexes (default: 'index')
- `--stopwords`: Path to stopwords file (default: stopwords.txt)
- `--index-type`: Type of index to generate (`unigram`, `bigram`, `trigram`, or `all`)

Analysis-Specific:

- `--log-file`: Path to the log file to analyze
- `--report-dir`: Directory for storing analysis reports (default: 'reports')
- `--report-format`: Report format (`text`, `html`, or `both`)

## Log Analysis Reports

The analysis stage generates detailed reports containing:

- Processing summary (documents processed, n-grams generated)
- Performance metrics (processing times, throughput)
- Memory usage trends
- Error patterns and frequencies
- State verification statistics

Reports can be generated in text format (for command-line viewing) and HTML format (for better visualization).

## Performance Considerations

1. Memory Usage:

   - Adjust batch size based on available memory
   - Monitor memory usage through log analysis
   - Use `--limit` for testing with smaller datasets

2. Processing Speed:

   - Adjust number of threads based on CPU cores
   - Use appropriate batch sizes (larger for faster processing, smaller for less memory)
   - Monitor throughput using log analysis

3. Storage:
   - Ensure sufficient disk space for indexes
   - Monitor index growth through log analysis
   - Consider using SSD for better I/O performance

## Troubleshooting

1. Out of Memory Errors:

   - Reduce batch size
   - Reduce number of threads
   - Ensure sufficient heap space with `-Xmx` JVM argument

2. Slow Processing:

   - Increase batch size if memory allows
   - Increase number of threads if CPU allows
   - Check disk I/O performance

3. Index Errors:
   - Check log files for specific error messages
   - Run analysis stage to identify error patterns
   - Verify input data quality

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
