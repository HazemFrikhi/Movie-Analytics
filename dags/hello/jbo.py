from pyspark.sql import SparkSession
import random
import string

def generate_random_data(num_lines=1000, max_words_per_line=10):
    """
    Generates random text data.
    :param num_lines: Number of lines to generate.
    :param max_words_per_line: Maximum number of words in each line.
    :return: List of random text lines.
    """
    data = []
    for _ in range(num_lines):
        line = " ".join(
            ''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 8)))  # Random word
            for _ in range(random.randint(1, max_words_per_line))                   # Number of words in line
        )
        data.append(line)
    return data

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("WordCountWithRandomData") \
        .getOrCreate()

    # Generate random data
    random_data = generate_random_data(num_lines=1000, max_words_per_line=10)

    # Parallelize random data into an RDD
    text_rdd = spark.sparkContext.parallelize(random_data)

    # Perform word count
    word_counts = (
        text_rdd.flatMap(lambda line: line.split(" "))  # Split each line into words
                .map(lambda word: (word, 1))           # Map each word to a count of 1
                .reduceByKey(lambda a, b: a + b)       # Reduce by key (word) to count occurrences
    )

    # Collect and display results (top 10 most common words)
    for word, count in word_counts.take(10):
        print(f"{word}: {count}")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()

