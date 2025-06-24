# PySpark Workshop: Batch Processing with Distributed Data
## PowerPoint Presentation Slides

---

## Slide 1: Title Slide

**BATCH PROCESSING WITH PYSPARK**
*Distributed Data Processing with Apache Spark*

🚀 Workshop for Data Engineers
⚡ Hands-on exercises with real code

**Presenter:** Sirojiddin Dushaev
**Date:** 26.06.2025

---

## Slide 2: Introduction to Distributed Data Processing

### 🚀 Apache Spark Overview

• **Apache Spark**: Open-source unified analytics engine for big data
• **Driver (master) + Worker (executor)** distributed computing model  
• **In-memory caching** for fast iterative algorithms
• **Fault tolerance** through RDD lineage and checkpointing
• **DAG-based execution** with lazy evaluation for optimization

💡 **Key Benefit**: Process terabytes of data across clusters with ease

---

## Slide 3: PySpark Batch Processing Concepts

### 🧠 Core Concepts

• **RDDs**: Low-level, partitioned, fault-tolerant data collections
• **DataFrames**: Schema-aware, optimized, SQL-like API for structured data
• **Transformations vs Actions**: Lazy operations vs execution triggers
• **DAG & Lazy Evaluation**: Efficient query planning and optimization
• **Catalyst Optimizer**: Automatic query optimization and code generation

### Example: Lazy Evaluation
```python
# No execution - just builds the plan
df.filter(col("age") > 30).select("name", "department")  

# Triggers execution
df.filter(col("age") > 30).select("name", "department").show()
```

---

## Slide 4: Exercise 1 - Reading, Transforming, Writing Data

### 💻 Hands-on Exercise 1

#### 🎯 Objectives:
• Read `people.csv` with proper schema definition
• Filter records where `age > 30`
• Add new column `age_doubled`
• Write results to output CSV

#### Key Code Concepts:
```python
# Reading data with schema
spark.read.csv("people.csv", header=True, schema=schema)

# Transformations
df.filter(col("age") > 30)
df.withColumn("age_doubled", col("age") * 2)

# Writing results
df.write.csv("output/results", header=True)
```

#### 🐳 Setup: Use our Docker environment - everyone gets the same results!

---

## Slide 5: Exercise 2 - Aggregation with groupBy and Join

### 🔗 Advanced Operations

#### 🎯 Exercise Objectives:
• Load `departments.csv` & `employees.csv`
• Join tables on `dept_id`
• Group by department and calculate `sum(salary)`
• Apply window functions for ranking

#### Key Code Concepts:
```python
# Joining DataFrames
employees_df.join(dept_df, "dept_id", "inner")

# Aggregations
joined_df.groupBy("dept_name").agg(
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary")
)

# Window functions
window_spec = Window.partitionBy("dept_name").orderBy(desc("salary"))
df.withColumn("rank", row_number().over(window_spec))
```

📊 **Real-world data aggregation patterns**

---

## Slide 6: Best Practices and Common Pitfalls

### ✅ DO THESE:
• Use `cache()` for reused DataFrames
• Define schemas explicitly
• Prefer built-in SQL functions over UDFs
• Use `show()`, `limit()` for data inspection
• Optimize joins/partitions using AQE

### 🚫 AVOID THESE:
• `collect()` on large datasets
• Unnecessary User Defined Functions (UDFs)
• Cartesian joins without proper conditions
• Too many small partitions
• Ignoring data skewness

### Optimization Example:
```python
# Enable Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

---

## Slide 7: Practical Tips & Resources

### 📚 Development Environment
• **Small public datasets** for learning (Titanic, flight delays)
• **Run PySpark locally** via Docker (our workshop setup)
• **Cloud platforms**: Databricks, AWS EMR, Google Dataproc
• **Interactive development**: Jupyter notebooks with PySpark

### ⚡ Performance & Tooling
• **File formats**: Prefer Parquet over CSV for better performance
• **Delta Lake**: ACID transactions and time travel
• **Monitoring**: Use Spark UI (localhost:4040) during development
• **pandas API on Spark**: Familiar syntax for pandas users

### 🐳 Our Workshop Environment:
**Docker + Jupyter + PySpark + Spark UI** - Everything ready to go!

---

## Slide 8: Wrap-Up Quiz & Q&A

### 🧩 Test Your Knowledge

**1.** What's the key difference between RDD and DataFrame?

**2.** When does Spark actually compute the results?

**3.** What is the main benefit of Catalyst optimizer?

**4.** Why should you avoid collect() on large datasets?

**5.** How does lazy evaluation help with performance?

### 🙋‍♀️ Q&A Session
• Error clarification and troubleshooting
• DAG and lazy evaluation deep dive
• Real-world implementation scenarios
• Performance optimization strategies

### 🎉 Thank You!
**Happy Spark-ing with big data!** ⚡

---

## Slide 9: Bonus - Docker Setup Command

### 🐳 Quick Setup for Participants

#### One Command Setup:
```bash
docker run -it --rm \
  -p 8888:8888 -p 4040:4040 \
  -v $(pwd):/home/jovyan/work \
  jupyter/pyspark-notebook:latest
```

#### Then open:
• **Jupyter Lab**: http://localhost:8888
• **Spark UI**: http://localhost:4040

#### What You Get:
✅ Complete PySpark environment
✅ No Java/Spark installation needed
✅ Same setup for everyone
✅ Easy cleanup after workshop

---

## Slide 10: Workshop Resources

### 📁 Files We'll Create:
• `people.csv` - Sample employee data
• `employees.csv` - Employee work details
• `departments.csv` - Department information
• `workshop.py` - Complete Python workshop code

### 🔗 Useful Links:
• **Apache Spark Documentation**: spark.apache.org
• **PySpark API Reference**: spark.apache.org/docs/latest/api/python/
• **Docker Desktop**: docker.com/products/docker-desktop
• **Jupyter Documentation**: jupyter.org

### 📧 Contact:
**dushaevsirojiddin@gmail.com**
**[GitHub](https://github.com/DushaevSirojiddin/pyspark-workshop)**

---

## Speaker Notes for Each Slide:

### Slide 1 Notes:
- Welcome everyone and introduce yourself
- Mention this is hands-on workshop
- Ensure everyone has Docker installed

### Slide 2 Notes:
- Explain distributed computing briefly
- Draw simple diagram of driver-worker model
- Emphasize fault tolerance benefits

### Slide 3 Notes:
- Show the difference between RDD and DataFrame
- Demonstrate lazy evaluation with simple example
- Explain why lazy evaluation is beneficial

### Slide 4 Notes:
- Live coding session starts here
- Make sure everyone can follow along
- Troubleshoot Docker issues if needed

### Slide 5 Notes:
- More complex operations
- Show Spark UI during execution
- Explain join strategies briefly

### Slide 6 Notes:
- Share real-world experiences
- Show performance differences with examples
- Mention monitoring tools

### Slide 7 Notes:
- Discuss production considerations
- Mention enterprise tools
- Share additional learning resources

### Slide 8 Notes:
- Interactive quiz time
- Encourage questions
- Summarize key learnings