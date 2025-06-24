# 🚀 Universal PySpark Workshop Setup with Docker

**Works on Mac, Windows, Linux - No Java/Spark installation needed!**

## 📋 Prerequisites (Only 2 things!)

1. **Docker Desktop** - [Download here](https://www.docker.com/products/docker-desktop/)
2. **Git** (optional) - Only if cloning from repository

## ⚡ Quick Start (3 Commands)

### Option 1: Super Simple (Recommended)
```bash
# 1. Create workshop folder
mkdir pyspark-workshop && cd pyspark-workshop

# 2. Download workshop files (copy the Python code from previous artifact)
# Save as workshop.py

# 3. Run PySpark with Docker (ONE COMMAND!)
docker run -it --rm \
  -p 8888:8888 -p 4040:4040 \
  -v $(pwd):/home/jovyan/work \
  jupyter/pyspark-notebook:latest \
  start-notebook.sh --NotebookApp.token='' --NotebookApp.password=''
```

### Option 2: Using Docker Compose (More Features)
```bash
# 1. Create project structure
mkdir pyspark-workshop && cd pyspark-workshop
mkdir workshop data

# 2. Create docker-compose.yml (copy from artifact above)

# 3. Start everything
docker-compose up
```

## 🖥️ Access Your Environment

After running the docker command, open your browser:

- **Jupyter Lab**: http://localhost:8888
- **Spark UI**: http://localhost:4040 (when Spark jobs are running)

## 📁 Project Structure
```
pyspark-workshop/
├── workshop.py          # Main workshop code
├── docker-compose.yml   # Docker setup (optional)
├── workshop/            # Your notebook files
├── data/               # Input/output data
└── README.md           # This file
```

## 🎯 Workshop Steps

1. **Start Docker container** (command above)
2. **Open Jupyter Lab** at http://localhost:8888
3. **Create new notebook** or upload workshop.py
4. **Run the workshop code** cell by cell
5. **Monitor Spark jobs** at http://localhost:4040

## 📝 Create Workshop Notebook

In Jupyter Lab, create a new notebook and paste this starter code:

```python
# Cell 1: Setup
import sys
sys.path.append('/home/jovyan/work')
from workshop import *

# Cell 2: Run Workshop
run_workshop()

# Cell 3: Individual Exercises
spark = create_spark_session()
create_sample_datasets()

# Cell 4: Exercise 1
exercise_1_basic_operations(spark)

# Cell 5: Exercise 2  
exercise_2_aggregation_and_joins(spark)

# Cell 6: Best Practices
demonstrate_best_practices(spark)

# Cell 7: Cleanup
spark.stop()
```

## 🛠️ Troubleshooting

### Mac Users
```bash
# If you get permission errors, use:
docker run -it --rm \
  -p 8888:8888 -p 4040:4040 \
  -v $(pwd):/home/jovyan/work \
  --user root \
  jupyter/pyspark-notebook:latest
```

### Windows Users (PowerShell)
```powershell
# Use PowerShell (not CMD)
docker run -it --rm `
  -p 8888:8888 -p 4040:4040 `
  -v ${PWD}:/home/jovyan/work `
  jupyter/pyspark-notebook:latest `
  start-notebook.sh --NotebookApp.token='' --NotebookApp.password=''
```

### Windows Users (CMD)
```cmd
docker run -it --rm ^
  -p 8888:8888 -p 4040:4040 ^
  -v %cd%:/home/jovyan/work ^
  jupyter/pyspark-notebook:latest ^
  start-notebook.sh --NotebookApp.token= --NotebookApp.password=
```

## 🔧 Customization Options

### Add More Python Packages
```bash
# Create a requirements.txt file:
echo "plotly
seaborn
matplotlib" > requirements.txt

# Run with custom packages:
docker run -it --rm \
  -p 8888:8888 -p 4040:4040 \
  -v $(pwd):/home/jovyan/work \
  jupyter/pyspark-notebook:latest \
  bash -c "pip install -r /home/jovyan/work/requirements.txt && start-notebook.sh --NotebookApp.token='' --NotebookApp.password=''"
```

### Persistent Data Storage
```bash
# Create persistent volume for data
docker volume create pyspark-data

# Run with persistent storage
docker run -it --rm \
  -p 8888:8888 -p 4040:4040 \
  -v $(pwd):/home/jovyan/work \
  -v pyspark-data:/home/jovyan/data \
  jupyter/pyspark-notebook:latest
```

## 🎓 For Workshop Participants

**Send this simple instruction to your workshop participants:**

> **Workshop Setup (2 minutes):**
> 1. Install Docker Desktop
> 2. Open terminal/command prompt
> 3. Run this command:
> ```bash
> docker run -it --rm -p 8888:8888 -p 4040:4040 jupyter/pyspark-notebook:latest
> ```
> 4. Open http://localhost:8888 in your browser
> 5. You're ready for PySpark! 🎉

## 📊 What You Get

✅ **Complete PySpark Environment**
- Apache Spark 3.x
- Python 3.x with PySpark
- Jupyter Lab interface
- All dependencies pre-installed

✅ **No Local Installation Needed**
- No Java installation
- No Scala installation  
- No Spark installation
- No environment conflicts

✅ **Cross-Platform**
- Works on Mac, Windows, Linux
- Same experience for everyone
- Easy cleanup (just stop container)

✅ **Workshop Ready**
- Pre-configured ports
- Spark UI accessible
- Volume mounting for code sharing

## 🚨 Production Notes

This setup is perfect for:
- ✅ Learning and workshops
- ✅ Development and testing
- ✅ Proof of concepts

For production use:
- Consider Kubernetes deployment
- Use managed Spark services (Databricks, EMR)
- Implement proper security measures

## 🤝 Sharing with Team

To share your workshop:
1. Create a GitHub repository
2. Include docker-compose.yml and workshop.py
3. Team members just need: `git clone && docker-compose up`

---

**That's it! Your universal PySpark workshop environment is ready! 🎉**