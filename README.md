# Introduction

The code in this repository provides sample PySpark functions and sample PyTest unit tests using Databricks Connect.

# Setting up

<details>
<summary><strong> Step 1: Create your Python environment </strong></summary>

Using conda, create a Python 3.11 environment:
```
conda create -n databricks_env python=3.11
```

Activate it:
```
conda activate databricks_env
```

Note:
- Python 3.11 is required to match Databricks Runtime 15.

</details>

<details>
<summary><strong> Step 2: Install dependencies </strong></summary>

```
pip install -r requirements.txt
```

</details>

<details>
<summary><strong> Step 3: Configure Databricks credentials </strong></summary>

Create or edit the file `~/.databrickscfg` and add the following:

```
[first_access_databrick_1]
host=https://your-workspace-url.azuredatabricks.net
token=your-personal-access-token
serverless_compute_id=auto
```

To generate a personal access token:
1. Open your Databricks workspace
2. Click your profile icon → Settings → Developer → Access Tokens
3. Click Generate new token
4. Copy the token

</details>

<details>
<summary><strong> Step 4: Validate Databricks Connect </strong></summary>

```
DATABRICKS_CONFIG_PROFILE=first_access_databrick_1 databricks-connect test
```

You should see:
```
* Checking Python version
* Creating and validating a session with the default configuration
* Testing the connection to the cluster - starts your cluster if it is not yet running
* Testing dbutils.fs
```

Note: The `dbutils.fs` check may fail if DBFS is disabled in your workspace — this is fine and does not affect running tests.

</details>

# Unit tests

Unit tests are performed using PyTest and chispa on your local machine, with Spark operations running on Databricks Serverless via Databricks Connect.

<details>
<summary><strong>Writing tests</strong></summary>

Use `DatabricksSession` instead of `SparkSession`:

```python
from databricks.connect import DatabricksSession
import chispa as cp
from ..cleaning_utils import *

def test_lowercase_all_columns():
    # ASSEMBLE
    test_data = [
        {"ID": 1, "First_Name": "Bob", "Last_Name": "Builder", "Age": 24},
        {"ID": 2, "First_Name": "Sam", "Last_Name": "Smith",   "Age": 41}
    ]

    spark = DatabricksSession.builder.getOrCreate()
    test_df = spark.createDataFrame(test_data)

    # ACT
    output_df = lowercase_all_column_names(test_df)

    # ASSERT
    expected_data = [
        {"id": 1, "first_name": "Bob", "last_name": "Builder", "age": 24},
        {"id": 2, "first_name": "Sam", "last_name": "Smith",   "age": 41}
    ]
    expected_df = spark.createDataFrame(expected_data)
    cp.assert_df_equality(output_df, expected_df, ignore_column_order=True)
```

The test does 3 things:

1. **Arrange**: Create a dummy Spark DataFrame
2. **Act**: Invoke the PySpark function
3. **Assert**: Check the result matches the expectation using chispa

</details>

<details>
<summary><strong>Running tests</strong></summary>

Set the Databricks profile and run pytest:

```
DATABRICKS_CONFIG_PROFILE=first_access_databrick_1 pytest functions -v
```

Or if you have added the profile to your `~/.zshrc`:

```
pytest functions -v
```

You should see:
```
collected 1 item
functions/tests/test_cleaning_utils.py::test_lowercase_all_columns PASSED [100%]
1 passed in 7.98s
```

</details>

# Continuous Integration (CI)

<details>
<summary><strong>GitHub Actions</strong></summary>

The CI pipeline is configured in `.github/workflows/databricks-ci.yml` and runs automatically on every push or pull request.

**Step 1: Create GitHub Secrets**

Go to your GitHub repo → Settings → Secrets and variables → Actions, and add:

- `DATABRICKS_HOST` → your workspace URL
- `DATABRICKS_TOKEN` → your personal access token

**Step 2: CI pipeline**

The pipeline will:
1. Spin up a Linux machine
2. Install dependencies from `requirements.txt`
3. Connect to Databricks Serverless
4. Run all tests with pytest
5. Publish test results to the PR page

</details>
