package TP1

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.math.BigDecimal.RoundingMode
import scala.util.Random


/***
  * ESIPE, "INFO 3-Opt° Logiciel"
  *
  * Calcul distribué (Map-Reduce Spark) (Autumn 2019) -- Instructors: F. Sarradin, B. Men
  *
  * Sources: These labs synthetize and builds on labs from several origins:
  *
    * The series of moocs from Berkeley and Databricks,(Creative Commons licences), namely
      * Introduction to Apache Spark => https://courses.edx.org/courses/course-v1:BerkeleyX+CS105x+1T2016/info
      * Big data Analysis with Apache Spark => https://courses.edx.org/courses/course-v1:BerkeleyX+CS110x+2T2016/info
      * Distributed Machine Learning with Apache Spark => https://courses.edx.org/courses/course-v1:BerkeleyX+CS120x+2T2016/info
      * Introduction to Big Data with Apache Spark => https://courses.edx.org/courses/BerkeleyX/CS100.1x/1T2015/info
      * Scalable Machine Learning => https://courses.edx.org/courses/BerkeleyX/CS190.1x/1T2015/info
    * Apache Spark & Python (pySpark) tutorials for Big Data Analysis and Machine Learning (Apache License, Version 2.0) => https://github.com/jadianes/spark-py-notebooks
  *
  * We have kept the labs text in english. This will enable us to reuse them in international sections.
  */

object Word_count_text {


  def main(args: Array[String]): Unit = {
/***
  * Lab 1 : Word Count with Spark
  */

/***
  * This lab will enable us to develop a simple word count application.
  * The volume of unstructured text in existence is growing dramatically, and Spark is an excellent tool for analyzing this type of data.
  * In this lab, we will write code that calculates the most common words in the Complete Works of William Shakespeare (http://www.gutenberg.org/ebooks/100) retrieved from Project Gutenberg (http://www.gutenberg.org/wiki/Main_Page).
  * This could also be scaled to find the most common words on the Internet.
  *
  * ** During this lab we will cover: **
  *
  * Part 1: Word Count with RDDs -- You will play with basic RDDs, then build a pair RDD, use it for counting words.
  * Finally you will learn how to clean and prepare the RDD and build the application to count words in a file.
  *
  * Part 2: is devoted to a small tutorial on Spark Dataframes (introduced in Spark 1.3)
  *
  * Part 3: Word Count with dataframes -- You will follow the same steps as in Part 1 to develop a Word Counting application and apply it to a file.
  *
  * Exercises will include an explanation of what is expected, followed by code cells where one cell will have one or more ??? sections.
  * The cell that needs to be modified will have // TODO: Replace ??? with appropriate code on its first line.
  * Once the ??? sections are updated and the code is run, the test cell can then be run to verify the correctness of your solution.
  * The last code cell before the next markdown section will contain the tests.
  *
  * Note that, for reference, you can look up the details of the relevant methods in Spark's Scala API => https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package
  */


/***
  * ********************************************
  * Prerequisites : Spark Context configuration
  * ********************************************
  * The Spark depedency have already been set in build.sbt, you can take a look
  */
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder
      .appName("lab1_word_count_text")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    LogManager.getRootLogger.setLevel(Level.ERROR)

/***
  * ********************************************
  * Part 1 : Word Count with RDDs
  * ********************************************
  *
  * --------------------------------------------
  * 1. Creating a base RDD and pair RDDs
  * --------------------------------------------
  *
  * We will first explore creating a base RDD with parallelize and using pair RDDs to count words.
  *
  * ** (1a) Create a base RDD **
  *
  * We'll start by generating a base RDD by using a List and the sc.parallelize method. Then we'll print out the type of the base RDD.
  */

    val wordsList = List("cat", "elephant", "rat", "rat", "cat")
    val wordsRDD = sc.parallelize(wordsList, 4)
    // Print out the type of wordsRDD
    println(wordsRDD.getClass)
    /***<DELETE this line if you finished last section with no errors>

/***
  * ** (1b) Pluralize and test **
  *
  * Let's use a map() transformation to add the letter 's' to each string in the base RDD we just created.
  * We'll define a function that returns the word with an 's' at the end of the word. Please replace ??? with your solution.
  * If you have trouble, the next cell has the solution. After you have defined makePlural you can run the third cell which contains a test.
  * If you implementation is correct, no exception will be thrown.
  *
  * This is the general form that exercises will take, except that no example solution will be provided.
   */

    // TODO: Replace ??? with appropriate code

    def makePluralTODO(word : String): String = {
      /*Adds an 's' to `word`.

      Note:
          This is a simple function that only adds an 's'.  No attempt is made to follow proper
          pluralization rules.

      Args:
          word (str): A string.

      Returns:
          str: A string with 's' added to it.
      */
      ???
    }

    //One way of completing the function above
    def makePlural(word : String): String =
      word + "s"

    println(makePlural("cat"))

    import org.scalatest.Assertions._ //importing the test library
    //This will do a test, an exception org.scalatest.exceptions.TestFailedException will be thrown if It is wrong
    assertResult("rats", "incorrect result: makePlural does not add an s") {
      makePlural("rat")
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * ** (1c) Apply makePlural to the base RDD **
  *
  * Now pass each item in the base RDD into a map() transformation that applies the makePlural() function to each element.
  * And then call the collect() action to see the transformed RDD.
  */

    // TODO: Replace ??? with appropriate code
    val pluralRDD = wordsRDD.map(???)
    pluralRDD.collect.foreach(println)

    // TEST Apply makePlural to the base RDD(1c)
    assertResult(Array("cats", "elephants", "rats", "rats", "cats"), "incorrect values for pluralRDD") {
      pluralRDD.collect
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * ** (1d) Pass a lambda function to map **
  *
  * Let's create the same RDD using a lambda function.
  */

    // TODO: Replace ??? with appropriate code
    val pluralLambdaRDD = wordsRDD.map(???)
    pluralLambdaRDD.collect.foreach(println)

    //TEST Pass a lambda function to map (1d)
    assertResult(List("cats", "elephants", "rats", "rats", "cats"), "incorrect values for pluralLambdaRDD (1d)") {
      pluralLambdaRDD.collect
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * ** (1e) Length of each word **
  *
  * Now use map() and a lambda function to return the number of characters in each word.
  * We'll collect this result directly into a variable.
  */

    // TODO: Replace ??? with appropriate code
    val pluralLengths = pluralRDD
      .???
      .collect
    pluralLengths.foreach(println)

    // TEST Length of each word (1e)
    assertResult(List(4, 9, 4, 4, 4), "incorrect values for pluralLengths") {
      pluralLengths
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>

/***
  * ** (1f) Pair RDDs **
  *
  * The next step in writing our word counting program is to create a new type of RDD, called a pair RDD.
  * A pair RDD is an RDD where each element is a pair tuple (k, v) where k is the key and v is the value.
  * In this example, we will create a pair consisting of ('<word>', 1) for each word element in the RDD.
  *
  * We can create the pair RDD using the map() transformation with a lambda() function to create a new RDD.
  */

    // TODO: Replace ??? with appropriate code
    val wordPairs = wordsRDD.???
    wordPairs.collect.foreach(println)

    // TEST Pair RDDs (1f)
    assertResult(List(("cat", 1), ("elephant", 1), ("rat", 1), ("rat", 1), ("cat", 1)), "incorrect values for wordPairs") {
      wordPairs.collect
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * --------------------------------------------
  * 2. Counting with pair RDDs
  * --------------------------------------------
  * Now, let's count the number of times a particular word appears in the RDD.
  * There are multiple ways to perform the counting, but some are much less efficient than others.
  *
  * A naive approach would be to collect() all of the elements and count them in the driver program.
  * While this approach could work for small datasets, we want an approach that will work for any size dataset including terabyte- or petabyte-sized datasets.
  * In addition, performing all of the work in the driver program is slower than performing it in parallel in the workers.
  * For these reasons, we will use data parallel operations.
  *
  * ** (2a) groupByKey() approach **
  *
  * An approach you might first consider (we'll see shortly that there are better ways) is based on using the groupByKey() transformation.
  * As the name implies, the groupByKey() transformation groups all the elements of the RDD with the same key into a single list in one of the partitions.
  * There are two problems with using groupByKey():
  *
  *   - The operation requires a lot of data movement to move all the values into the appropriate partitions.
  *   - The lists can be very large. Consider a word count of English Wikipedia: the lists for common words (e.g., the, a, etc.)
  *     would be huge and could exhaust the available memory in a worker.
  *
  * Use groupByKey() to generate a pair RDD of type ('word', Iterable).
  */
    // TODO: Replace ??? with appropriate code
    // Note that groupByKey requires no parameters
    val wordsGrouped = wordPairs.???
    val keyValues = wordsGrouped.collect
    keyValues.foreach(x => println(x._1,x._2))

    // TEST groupByKey() approach (2a)
    assertResult(Array(("cat", List(1, 1)), ("elephant", List(1)), ("rat", List(1, 1))),
    "incorrect value for wordsGrouped") {
      wordsGrouped.mapValues(_.toList).collect.sortBy(_._1)
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * ** (2b) Use groupByKey() to obtain the counts **
  *
  * Using the `groupByKey()` transformation creates an RDD containing 3 elements, each of which is a pair of a word and a Iterable.
  * Now sum the Iteraable using a `map()` transformation. The result should be a pair RDD consisting of (word, count) pairs.
  */

    // TODO: Replace ??? with appropriate code
    val wordCountsGrouped = wordsGrouped.???
    wordCountsGrouped.collect.foreach(println)

    // TEST Use groupByKey() to obtain the counts (2b)
    assertResult(Array(("cat", 2), ("elephant", 1), ("rat", 2)),
    "incorrect value for wordCountsGrouped") {
      wordCountsGrouped.collect.sortBy(_._1)
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * ** (2c) Counting using reduceByKey **
  *
  * A better approach is to start from the pair RDD and then use the reduceByKey() transformation to create a new pair RDD.
  * The reduceByKey() transformation gathers together pairs that have the same key and applies the function provided to two values at a time, iteratively reducing all of the values to a single value.
  * reduceByKey() operates by applying the function first within each partition on a per-key basis and then across the partitions, allowing it to scale efficiently to large datasets.
  */

    // TODO: Replace ??? with appropriate code
    // Note that reduceByKey takes in a function that accepts two values and returns a single value
    val wordCounts = wordPairs.reduceByKey(???)
    wordCounts.collect.foreach(println)

    // TEST Counting using reduceByKey (2c)
    assertResult(Array(("cat", 2), ("elephant", 1), ("rat", 2)),
      "incorrect value for wordCounts") {
      wordCounts.collect.sortBy(_._1)
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * ** (2d) All together **
  *
  * The expert version of the code performs the map() to pair RDD, reduceByKey() transformation, and collect in one statement.
  */

    // TODO: Replace ??? with appropriate code
    val wordCountsCollected = wordsRDD
                              .???
      .collect()
    wordCountsCollected.foreach(println)

    // TEST All together (2d)
    assertResult(Array(("cat", 2), ("elephant", 1), ("rat", 2)),
    "incorrect value for wordCountsCollected") {
      wordCountsCollected.sortBy(_._1)
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * --------------------------------------------
  * 3. Finding unique words and a mean value
  * --------------------------------------------
  * ** (3a) Unique words **
  *
  * Calculate the number of unique words in wordsRDD. You can use other RDDs that you have already created to make this easier.
  */

    // TODO: Replace ??? with appropriate code
    val uniqueWords = ???
    println(uniqueWords)

    // TEST Unique words (3a)
    assertResult(3, "incorrect count of uniqueWords") {
      uniqueWords
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
  * ** (3b) Mean using reduce **
  *
  * Find the mean number of words per unique word in `wordCounts`.
  *
  * Use a reduce() action to sum the counts in wordCounts and then divide by the number of unique words.
  * First map() the pair RDD wordCounts, which consists of (key, value) pairs, to an RDD of values.
  */

    // TODO: Replace ??? with appropriate code
    val totalCount = (wordCounts
      .map(???)
      .reduce(???))
    val average = totalCount / ???.toFloat
    println(totalCount)
    println(BigDecimal(average).setScale(2, RoundingMode.HALF_UP))

    // TEST Mean using reduce (3b)
    assertResult( BigDecimal(1.67), "incorrect value of average") {
      BigDecimal(average).setScale(2, RoundingMode.HALF_UP)
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * --------------------------------------------
 * 4. Apply word count to a file
 * --------------------------------------------
 * In this section we will finish developing our word count application.
 * We'll have to build the wordCount function, deal with real world problems like capitalization and punctuation,
 * load in our data source, and compute the word count on the new data.
 *
 * ** (4a) wordCount function **
 *
 * First, define a function for word counting.
 * You should reuse the techniques that have been covered in earlier parts of this lab.
 * This function should take in an RDD that is a list of words like wordsRDD and return a pair RDD that has all of the words and their associated counts.
 */

    // TODO: Replace <FILL IN> with appropriate code
    def wordCount(wordListRDD: RDD[String]) : RDD[(String,Int)] = {
      /*Creates a pair RDD with word counts from an RDD of words.

      Args:
      wordListRDD (RDD of str): An RDD consisting of words.

      Returns:
      RDD of (str, int): An RDD consisting of (word, count) tuples.
      */
      ???
    }

    wordCount(wordsRDD).collect.foreach(println)

    // TEST wordCount function (4a)
    assertResult(Array(("cat", 2), ("elephant", 1), ("rat", 2)),
    "incorrect definition for wordCount function") {
      wordCount(wordsRDD).collect.sortBy(_._1)
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** (4b) Capitalization and punctuation **
 *
 * Real world files are more complicated than the data we have been using in this lab.
 * Some of the issues we have to address are:
 *
 *  - Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
 *  - All punctuation should be removed.
 *  - Any leading or trailing spaces on a line should be removed.
 *
 * Define the function removePunctuation that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces.
 * Use replaceAll to remove any text that is not a letter, number, or space.
 */
    // TODO: Replace <FILL IN> with appropriate code
    def removePunctuation(text:String) = {
      /*
    Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
       */

      val text_no_punctuation = text.replaceAll("[^a-zA-Z0-9 ]+","")
      ???
    }

    println(removePunctuation("Hi, you!"))
    println(removePunctuation(" No under_score!"))

    // TEST Capitalization and punctuation (4b)
    assertResult("the elephants 4 cats",
      "incorrect definition for removePunctuation function") {
      removePunctuation(" The Elephant's 4 cats. ")
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>

/***
 * ** (4c) Load a text file **
 *
 * For the next part of this lab, we will use the Complete Works of William Shakespeare (http://www.gutenberg.org/ebooks/100)
 * from Project Gutenberg (http://www.gutenberg.org/wiki/Main_Page).
 * To convert a text file into an RDD, we use the SparkContext.textFile() method.
 * We also apply the recently defined removePunctuation() function using a map() transformation to strip out the punctuation and change all text to lowercase.
 * Since the file is large we use take(15), so that we only print 15 lines.
 */
    // Just run this code
    val fileName = "Resources/shakespeare.txt"

    val shakespeareRDD = (sc
      .textFile(fileName, 8)
      .map(removePunctuation))

    shakespeareRDD
      .zipWithIndex()  // to (line, lineNum)
      .map(x => s"${x._2}:${x._1}")  // to 'lineNum: line'
    .take(15).foreach(println)
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** (4d) Words from lines **
 *
 * Before we can use the wordcount() function, we have to address two issues with the format of the RDD:
 *
 *  - The first issue is that that we need to split each line by its spaces.
 *  - The second issue is we need to filter out empty lines.
 *
 * Apply a transformation that will split each element of the RDD by its spaces.
 * For each element of the RDD, you should apply the string split() function.
 * You might think that a map() transformation is the way to do this, but think about what the result of the split() function will be.
 * Use an other transformation.
 */
    // TODO: Replace ??? with appropriate code
    val shakespeareWordsRDD = shakespeareRDD.???
    val shakespeareWordCount = shakespeareWordsRDD.count
    shakespeareWordsRDD.top(5).foreach(println)
    println(shakespeareWordCount)

    // TEST Words from lines (4d)
    // This test allows for leading spaces to be removed either before or after punctuation is removed.
    assertResult(true, "incorrect value for shakespeareWordCount") {
      shakespeareWordCount == 927631 || shakespeareWordCount == 928908
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** (4e) Remove empty elements **
 *
 * The next step is to filter out the empty elements. Remove all entries where the word is "".
 */
    // TODO: Replace ??? with appropriate code
    val shakeWordsRDD = shakespeareWordsRDD.???
    val shakeWordCount = shakeWordsRDD.count
    println(shakeWordCount)

    // TEST Remove empty elements (4e)
    assertResult(882996 , "incorrect value for shakeWordCount") {
      shakeWordCount
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** (4f) Count the vocabulary size **
 *
 * How many different words (vocabulary size) are there in Shakespeare's vocabulary?
 */
    // let us get the complete Shakespare' vocabulary
    val shakespare_vocab_size = ???
    println(s"Shakespeare vocabulary contains $shakespare_vocab_size words")

    // TEST Count the vocabulary size (4f)
    assertResult(28147, "incorrect value for shakespare_vocab_size") {
      shakespare_vocab_size
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** (4g) Get the top words **
 *
 * We now have an RDD that is only words.
 * Next, let's apply the wordCount() function to produce a list of word counts. We can view the top 15 words by using the takeOrdered() action; however, since the elements of the RDD are pairs, we need a custom sort function that sorts using the value part of the pair.
 *
 * You'll notice that many of the words are common English words.
 * These are called stopwords. In a later lab, we will see how to eliminate them from the results.
 * Use the `wordCount()` function and `takeOrdered()` to obtain the fifteen most common words and their counts.
 */
    // TODO Replace ??? with appropriate code
    val top15WordsAndCounts = ???
    top15WordsAndCounts.foreach(x => println(s"${x._1}: ${x._2}"))

    // TEST Count the words (4f)
    assertResult(
      Array(("the", 27361), ("and", 26028), ("i", 20681), ("to", 19150), ("of", 17463),
    ("a", 14593), ("you", 13615), ("my", 12481), ("in", 10956), ("that", 10890),
    ("is", 9134), ("not", 8497), ("with", 7771), ("me", 7769), ("it", 7678)),
    "incorrect value for top15WordsAndCounts") {
      top15WordsAndCounts
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ********************************************
 * Part 2 : Short tutorial to DataFrames
 * ********************************************
 * and chaining together transformations and actions
 *
 * --------------------------------------------
 * 1. Working with your first DataFrames
 * --------------------------------------------
 *
 * In Spark, we first create a base DataFrame (http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.package@DataFrame=org.apache.spark.sql.Dataset[org.apache.spark.sql.Row])
 * We can then apply one or more transformations to that base DataFrame.
 * A DataFrame is immutable, so once it is created, it cannot be changed.
 * As a result, each transformation creates a new DataFrame.
 * Finally, we can apply one or more actions to the DataFrames.
 *
 *    Note that Spark uses lazy evaluation, so transformations are not actually executed until an action occurs.
 *
 * We will perform several exercises to obtain a better understanding of DataFrames:
 *
 *  - Create a collection of 10,000 people
 *  - Create a Spark DataFrame from that collection
 *  - Subtract one from each value using map
 *  - Perform action collect to view results
 *  - Perform action count to view counts
 *  - Apply transformation filter and view results with collect
 *  - Learn about lambda functions
 *  - Explore how lazy evaluation works and the debugging challenges that it introduces
 *
 * A DataFrame consists of a series of Row objects; each Row object has a set of named columns.
 * You can think of a DataFrame as modeling a table, though the data source being processed does not have to be a table.
 *
 * More formally, a DataFrame must have a schema, which means it must consist of columns, each of which has a name and a type.
 * Some data sources have schemas built into them. Examples include RDBMS databases, Parquet files, and NoSQL databases like Cassandra.
 * Other data sources don't have computer-readable schemas, but you can often apply a schema programmatically.
 *
 * --------------------------------------------
 * 2. Create a collection of 10,000 people
 * --------------------------------------------
 *
 */
    import faker._
    import spark.implicits._

/***
 * We're going to create a collection of randomly generated people records.
 * In the next section, we'll turn that collection into a DataFrame.
 * We'll use the spark.implicits._, because that will give us helpers to create a DataFrame from a List for example.
 * There are other ways to define schemas, though; see the Spark Programming Guide's discussion of schema inference for more information.
 * (http://spark.apache.org/docs/latest/sql-programming-guide.html#inferring-the-schema-using-reflection)
 */

    val random = new Random()

    val data = for (n <- 1 to 10000) yield (Name.first_name,Name.last_name, PhoneNumber.phone_number, random.nextInt(90)+1)
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>

/***
 * --------------------------------------------
 * 3. Distributed data and using a collection to create a DataFrame
 * --------------------------------------------
 * In Spark, datasets are represented as a list of entries, where the list is broken up into many different partitions that are each stored on a different machine.
 * Each partition holds a unique subset of the entries in the list. Spark calls datasets that it stores "Resilient Distributed Datasets" (RDDs).
 * Even DataFrames are ultimately represented as RDDs, with additional meta-data.
 *
 * One of the defining features of Spark, compared to other data analytics frameworks (e.g., Hadoop), is that it stores data in memory rather than on disk.
 * This allows Spark applications to run much more quickly, because they are not slowed down by needing to read data from disk.
 * The figure to the right illustrates how Spark breaks a list of data entries into partitions that are each stored in memory on a worker.
 *
 * See the figure here http://spark-mooc.github.io/web-assets/images/cs105x/diagram-3b.png
 *
 * To create the DataFrame, we'll use `toDF` from our array of data.
 * Spark will create a new set of input data based on data that is passed in.
 * A DataFrame requires a _schema_, which is a list of columns, where each column has a name and a type.
 * Our list of data has elements with types (mostly strings, but one integer).
 * We'll supply the rest of the schema and the column names as the argument to `toDF`.
 */

    val dataDF = data.toDF("first_name","last_name","phone_number","age")

    //Let's take a look at the DataFrame's schema and some of its rows.
    dataDF.printSchema()
    //How many partitions will the DataFrame be split into?
    println(dataDF.rdd.getNumPartitions)
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>

/***
 * --------------------------------------------
 * 4. Subtract one from each value using select
 * --------------------------------------------
 * So far, we've created a distributed DataFrame that is splitted into many partitions, where each partition is stored on a single machine in our cluster.
 * Let's look at what happens when we do a basic operation on the dataset. Many useful data analysis operations can be specified as "do something to each item in the dataset".
 * These data-parallel operations are convenient because each item in the dataset can be processed individually:
 * the operation on one entry doesn't effect the operations on any of the other entries.
 * Therefore, Spark can parallelize the operation.
 *
 * One of the most common DataFrame operations is select(), and it works more or less like a SQL SELECT statement:
 * You can select specific columns from the DataFrame, and you can even use select() to create new columns with values that are derived from existing column values.
 * We can use select() to create a new column that decrements the value of the existing age column.
 *
 * select() is a transformation. It returns a new DataFrame that captures both the previous DataFrame and the operation to add to the query (select, in this case).
 * But it does not actually execute anything on the cluster. When transforming DataFrames, we are building up a query plan.
 * That query plan will be optimized, implemented (in terms of RDDs), and executed by Spark only when we call an action (see Lazy Evaluation process).
 */
    //Transform dataDF through a select transformation and rename the newly created '(age -1)' column to 'age'
    //Because select is a transformation and Spark uses lazy evaluation, no jobs, stages, or tasks will be launched when we run this code.
    val subDF = dataDF.select(dataDF("last_name"), dataDF("first_name"), (dataDF("age") - 1).alias("age"))
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>

/***
 * --------------------------------------------
 * 5. Use collect or show to view results
 * --------------------------------------------
 * To see a list of elements decremented by one, we need to create a new list on the driver from the the data distributed in the executor nodes.
 * To do this we can call the collect() method on our DataFrame. collect() is often used after transformations to ensure that we are only returning a small amount of data to the driver.
 * This is done because the data returned to the driver must fit into the driver's available memory. If not, the driver will crash.
 *
 * The collect() method is the first action operation that we have encountered.
 * Action operations cause Spark to perform the (lazy) transformation operations that are required to compute the values returned by the action.
 * In our example, this means that tasks will now be launched to perform the createDataFrame, select, and collect operations.
 *
 * See the diagram http://spark-mooc.github.io/web-assets/images/cs105x/diagram-3d.png
 *
 * In the diagram, the dataset is broken into four partitions, so four collect() tasks are launched.
 * Each task collects the entries in its partition and sends the result to the driver, which creates a list of the values, as shown in the figure below.
 *
 * Now let's run collect() on subDF.
 */

    subDF.take(3).foreach(println)
    // Let's collect the data
    val results = subDF.collect
    results.foreach(println)

    subDF.show() //Look at parameters
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * --------------------------------------------
 * 6. Use count to get total
 * --------------------------------------------
 * One of the most basic jobs that we can run is the count() job which will count the number of elements in a DataFrame, using the count() action.
 * Since select() creates a new DataFrame with the same number of elements as the starting DataFrame,
 * we expect that applying count() to each DataFrame will return the same result.
 *
 * Note that because count() is an action operation, if we had not already performed an action with collect(),
 * then Spark would now perform the transformation operations when we executed count().
 *
 * Each task counts the entries in its partition and sends the result to your SparkContext, which adds up all of the counts.
 * The figure down below shows what would happen if we ran count() on a small example dataset with just four partitions.
 *
 * http://spark-mooc.github.io/web-assets/images/cs105x/diagram-3e.png
 */
    val dataDFCount = dataDF.count
    val subDFCount = subDF.count
    println(dataDFCount)
    println(subDFCount)
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>

/***
 * --------------------------------------------
 * 7. Apply transformation filter and view results with collect
 * --------------------------------------------
 * Next, we'll create a new DataFrame that only contains the people whose ages are less than 10.
 * To do this, we'll use the filter() transformation. (You can also use where(), an alias for filter(), if you prefer something more SQL-like).
 * The filter() method is a transformation operation that creates a new DataFrame from the input DataFrame, keeping only values that match the filter expression.
 *
 * The figure shows how this might work on the small four-partition dataset.
 * http://spark-mooc.github.io/web-assets/images/cs105x/diagram-3f.png
 *
 * To view the filtered list of elements less than 10, we need to create a new list on the driver from the distributed data on the executor nodes.
 * We use the collect() method to return a list that contains all of the elements in this filtered DataFrame to the driver program.
 */

    val filteredDF = subDF.filter(subDF("age") < 10)
    filteredDF.show(truncate = false)
    println(filteredDF.count)
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>

/***
 * --------------------------------------------
 * 8. Additional DataFrame actions
 * --------------------------------------------
 * Let's investigate some additional actions:
 *  - first() => https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset@first():T
 *  - take() => https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset@take(n:Int):Array[T]
 * One useful thing to do when we have a new dataset is to look at the first few entries to obtain a rough idea of what information is available.
 * In Spark, we can do that using actions like first(), take(), and show().
 * Note that for the first() and take() actions, the elements that are returned depend on how the DataFrame is partitioned.
 *
 * Instead of using the collect() action, we can use the take(n) action to return the first n elements of the DataFrame.
 * The first() action returns the first element of a DataFrame, and is equivalent to take(1)[0].
 */
    println(s"first: ${filteredDF.first}")
    println("Four of them:")
    filteredDF.take(4).foreach(println)
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>

/***
 * --------------------------------------------
 * 9. Additional DataFrame transformations
 * --------------------------------------------
 * ** _orderBy_**
 *
 * orderBy() allows you to sort a DataFrame by one or more columns, producing a new DataFrame.
 *
 * For example, let's get the first five oldest people in the original (unfiltered) DataFrame.
 * We can use the orderBy() transformation. orderBy takes one or more columns, either as names (strings) or as Column objects.
 * To get a Column object, we use one notation on the DataFrame:
 *
 *  - filteredDF("age")
 *
 * This syntax returns a Column, which has additional methods like desc() (for sorting in descending order) or asc() (for sorting in ascending order, which is the default).
 *
 * Here are some examples:
 *  - dataDF.orderBy(dataDF("age"))  // sort by age in ascending order; returns a new DataFrame
 *  - dataDF.orderBy(dataDF("last_name").desc()) // sort by last name in descending order
 */
    //Get the five oldest people in the list. To do that, sort by age in descending order.
    dataDF.orderBy(dataDF("age").desc).take(5).foreach(println)
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** distinct and _dropDuplicates_**
 *
 * distinct() filters out duplicate rows, and it considers all columns.
 * Since our data is completely randomly generated, it's extremely unlikely that there are any duplicate rows:
 */

    val dataDFDistinctCount = dataDF.distinct.count
    println(dataDFCount)
    println(dataDFDistinctCount)

/***
 * ** _drop_**
 *
 * drop() is like the opposite of select(): Instead of selecting specific columns from a DataFrame, it drops a specifed column from a DataFrame.
 *
 * Here's a simple use case: Suppose you're reading from a 1,000-column CSV file, and you have to get rid of five of the columns.
 * Instead of selecting 995 of the columns, it's easier just to drop the five you don't want.
 */

/***
 * groupBy
 *
 * [groupBy()]((https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset@groupBy(cols:org.apache.spark.sql.Column*):org.apache.spark.sql.RelationalGroupedDataset) is one of the most powerful transformations.
 * It allows you to perform aggregations on a DataFrame.
 *
 * Unlike other DataFrame transformations, groupBy() does not return a DataFrame.
 * Instead, it returns a special GroupedData object that contains various aggregation functions.
 *
 * The most commonly used aggregation function is count(), but there are others (like sum(), max(), and avg().
 *
 * These aggregation functions typically create a new column and return a new DataFrame.
 */
     dataDF.groupBy("age").count().show(truncate = false)

    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ********************************************
 * Part 3 : WordCount with Dataframes
 * ********************************************
 *
 * Create a DataFrame
 *
 * We'll start by generating a base DataFrame by using a List of Strings and the toDF method.
 * Then we'll print out the type and schema of the DataFrame
 *
 */
    val wordsDF = List("cat","elephant","rat","rat","cat").toDF("word")
    wordsDF.show()
    println(wordsDF.getClass)
    wordsDF.printSchema()
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * --------------------------------------------
 * 1. Counting with Spark SQL and DataFrames
 * --------------------------------------------
 *
 * Now, let's count the number of times a particular word appears in the 'word' column.
 * There are multiple ways to perform the counting, but some are much less efficient than others.
 *
 * A naive approach would be to call collect on all of the elements and count them in the driver program.
 * While this approach could work for small datasets, we want an approach that will work for any size dataset including terabyte- or petabyte-sized datasets.
 * In addition, performing all of the work in the driver program is slower than performing it in parallel in the workers.
 * For these reasons, we will use data parallel operations.
 *
 * Using groupBy and count
 *
 * Using DataFrames, we can perform aggregations by grouping the data using the groupBy function on the DataFrame.
 * Using groupBy returns a GroupedData object and we can use the functions available for GroupedData to aggregate the groups.
 * For example, we can call avg or count on a GroupedData object to obtain the average of the values in the groups or the number of occurrences in the groups, respectively.
 *
 * To find the counts of words, group by the words and then use the count function to find the number of times that words occur.
 */
    // TODO: Replace ??? with appropriate code
    val wordCountsDF = wordsDF.???
    wordCountsDF.show()

    // TEST groupBy and count (2a)
    assertResult(Array(("cat",2), ("elephant",1), ("rat", 2)), "incorrect counts for wordCountsDF") {
      wordCountsDF.collect().map({case Row(s : String, c : Long) => (s,c)}).sortBy(_._1)
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * --------------------------------------------
 * 2. Finding unique words and a mean value
 * --------------------------------------------
 * ** Unique words **
 *
 * Calculate the number of unique words in wordsDF.
 * You can use other DataFrames that you have already created to make this easier.
 */
    // TODO: Replace ??? with appropriate code
    val uniqueWordsCount = ???
    println(uniqueWordsCount)

    // TEST Unique words
    assertResult(3, "incorrect count of unique words") {
      uniqueWordsCount
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * --------------------------------------------
 * Apply word count to a file
 * --------------------------------------------
 * In this section we will finish developing our word count application.
 * We'll have to build the wordCount function, deal with real world problems like capitalization and punctuation, load in our data source, and compute the word count on the new data.
 *
 * ** The wordCount function **
 *
 * First, define a function for word counting. You should reuse the techniques that have been covered in earlier parts of this lab.
 * This function should take in a DataFrame that is a list of words like wordsDF and return a DataFrame that has all of the words and their associated counts.
 *
 */
    // TODO: Replace ??? with appropriate code
    def wordCountDF(wordListDF : DataFrame) = {
      /*
    Creates a DataFrame with word counts.

    Args:
        wordListDF (DataFrame of str): A DataFrame consisting of one string column called 'word'.

    Returns:
        DataFrame of (str, int): A DataFrame containing 'word' and 'count' columns.
       */
     ???
    }
    wordCountDF(wordsDF).show()

    // TEST wordCount function (4a)
    assertResult(Array(("cat", 2), ("elephant", 1), ("rat", 2)), "incorrect definition for wordCountDF function") {
      wordCountDF(wordsDF).collect().map({case Row(s : String, c : Long) => (s,c)}).sortBy(_._1)
    }
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** Capitalization and punctuation **
 *
 * Real world files are more complicated than the data we have been using in this lab.
 * Some of the issues we have to address are:
 *
 *  - Words should be counted independent of their capitialization (e.g., Spark and spark should be counted as the same word).
 *  - All punctuation should be removed.
 *  - Any leading or trailing spaces on a line should be removed.
 *
 * Define the function removePunctuation that converts all text to lower case, removes any punctuation, and removes leading and trailing spaces.
 *
 * To complete this task, you should use Pyspark implemented functions such as :
 *
 * regexp_replace module to remove any text that is not a letter, number, or space.
 * If you are unfamiliar with regular expressions, refer to the removePunctuation function defined previously in this notebook.
 * You may also want to review this tutorial from Google. Also, this website is a great resource for debugging your regular expression.
 *
 * You should also use the trim and lower functions found in [pyspark.sql.functions] in order to lower text and remove leading and trailing spaces.
 *
 *    Note that you shouldn't use any RDD operations or need to create custom user defined functions (udfs) to accomplish this task
 */
    // TODO: Replace ??? with appropriate code
    import org.apache.spark.sql.functions._
    def removePunctuationDF(coll : Column) = {
      /*
    Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        column (Column): A Column containing a sentence.

    Returns:
        Column: A Column named 'sentence' with clean-up operations applied.
       */

      lower(trim(regexp_replace(coll, "[^\\w\\s\\d]*","")))
    }

    val sentenceDF = List(("Hi, you!"),(" No under_score!"), (" *      Remove punctuation then spaces  * "))
      .toDF("sentence")

    sentenceDF.show(truncate = false)
    sentenceDF
      .select(removePunctuationDF(sentenceDF("sentence")))
      .show(truncate = false)
    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** Load a text file **
 *
 * For the next part of this lab, we will use the Complete Works of William Shakespeare from Project Gutenberg.
 * To convert a text file into a DataFrame, we use the sqlContext.read.text() method.
 * We also apply the recently defined removePunctuation() function using a select() transformation to strip out the punctuation and change all text to lower case.
 * Since the file is large we use show(15), so that we only print 15 lines.
 */
    val shakespeareDF = spark.sqlContext.read.text(fileName).select(removePunctuationDF(col("value")))
    shakespeareDF.show(15, truncate = false)

    *//***<DELETE ME>*/
    /***<DELETE this line if you finished last section with no errors>
/***
 * ** Words from lines **
 *
 * Before we can use the wordcount() function, we have to address two issues with the format of the DataFrame:
 *
 *  - The first issue is that that we need to split each line by its spaces.
 *  - The second issue is we need to filter out empty lines or words.
 *
 * Apply a transformation that will split each 'sentence' in the DataFrame by its spaces,
 * and then transform from a DataFrame that contains lists of words into a DataFrame with each word in its own row.
 * To accomplish these two tasks you can use the split and explode functions.
 *
 * Once you have a DataFrame with one word per row you can apply the DataFrame operation where to remove the rows that contain ''.
 *
 *    Note that shakeWordsDF should be a DataFrame with one column named word.
 */
    // TODO: Replace ??? with appropriate code
    val shakeWordsDF :DataFrame = (shakespeareDF.???)
    shakeWordsDF.show()
    val shakeWordsDFCount = shakeWordsDF.count()
    println(shakeWordsDFCount)

    // TEST Remove empty elements (4d)
    assertResult(882996, "incorrect value for shakeWordCount") {
      shakeWordsDFCount
    }
    assertResult(Array("word"),"shakeWordsDF should only contain the Column 'word'") {
      shakeWordsDF.columns
    }
  *//***<DELETE ME>*/
  /***<DELETE this line if you finished last section with no errors>
/***
 * ** Count the words **
 *
 * We now have a DataFrame that is only words.
 * Next, let's apply the wordCount() function to produce a list of word counts.
 * We can view the first 20 words by using the show() action;
 * however, we'd like to see the words in descending order of count, so we'll need to apply the orderBy DataFrame method to first sort the DataFrame that is returned from wordCount().
 *
 * You'll notice that many of the words are common English words.
 * These are called stopwords. In a later lab, we will see how to eliminate them from the results.
 */

    // TODO: Replace ??? with appropriate code
    val topWordsAndCountsDF = ???
    topWordsAndCountsDF.show()

    // TEST Count the words (4e)
    assertResult(
      Array(("the", 27361), ("and", 26028), ("i", 20681), ("to", 19150), ("of", 17463),
        ("a", 14593), ("you", 13615), ("my", 12481), ("in", 10956), ("that", 10890),
        ("is", 9134), ("not", 8497), ("with", 7771), ("me", 7769), ("it", 7678)),
      "incorrect value for top15WordsAndCountsDF") {
      topWordsAndCountsDF.take(15).map({
        case Row(s:String,c:Long) => (s,c)
      })
    }
   *//***<DELETE ME>*/
  }
}
