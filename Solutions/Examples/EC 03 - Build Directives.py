# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Build Directives
# MAGIC This notebook demonstrates different patterns common to notebook-based development.
# MAGIC 
# MAGIC These API behind these directives is typically pip-installed by the publishing scripts but is equivilent to the following statement cell

# COMMAND ----------

# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@main --quiet --disable-pip-version-check

# COMMAND ----------

from dbacademy_courseware import help_html
html = help_html()
displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Header and Footer Directives
# MAGIC 
# MAGIC Each notebook is required to have a pair of header directives in the first cell of each notebook.
# MAGIC * The first is the header directive and must be one of the following two values:
# MAGIC     * **`INCLUDE_HEADER_FALSE`** - No header is added to the published notebook. This is typical for notebooks in the **`/Includes`** folder such as the Classroom-Setup scripts.
# MAGIC     * **`INCLUDE_HEADER_TRUE`** - Adds the standard Databricks Academy header to the first cell of each published notebook.
# MAGIC * The the header directive, the footer directive must be one of the following two values:
# MAGIC     * **`INCLUDE_FOOTER_FALSE`** - No footer is atted to the the published notebook.
# MAGIC     * **`INCLUDE_FOOTER_TRUE`** - Adds the standard footer to the last cell of each published notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## i18n Directive
# MAGIC 
# MAGIC The i18n directive is located at the top of each MD cell.
# MAGIC * This is only required if the course is being translated into multiple languages.
# MAGIC * If the course is not being translated, these can simply be removed.
# MAGIC * If this course is to be translated, it might be advisable to use existing scripts to inject the directives as oposed to having to generate them by hand.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The DUMMY Directive
# MAGIC 
# MAGIC This directive is provided only for testing purposes and should never appear in a course.

# COMMAND ----------

# MAGIC %md
# MAGIC -- DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive
# MAGIC 
# MAGIC In the published version of this notebook, the word "DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive" above will be replaced with the following statement:
# MAGIC > DUMMY: Ya, that wasn't too smart. Then again, this is just a dummy-directive: Ya, that wasn't too smart. Then again, this is just a dummy-directive

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The SOURCE_ONLY Directive
# MAGIC 
# MAGIC When added to a code cell (Python, Scala, R or SQL), the build tool will exclude the respective cell from the published notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The TODO Directive
# MAGIC 
# MAGIC This directive has several key features:
# MAGIC * Must be added to a code cell (Python, Scala, R or SQL)
# MAGIC * In the source notebook, every line must be commented out (as seen below) so that it is not executed from this source-notebook
# MAGIC * When published this directive affects two notebooks:
# MAGIC     * The Student Notebook: added to the same relative path as this notebook however, the command will be uncommented with the expectation that it may not compile.
# MAGIC     * The Solutions Notebook: the published version of this notebook will be republished under the **/Solutions** folder but in the case of this directive, the corresponding command will be excluded entirely.
# MAGIC * The expectation is that this cell will not compile because it is missing key code fragmetns expected to be completed by the student.
# MAGIC * The convention is to use the **`FILL_IN`** keyword which is expected to be removed by the student and replaced with the appropariate code.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The ANSWER Directive
# MAGIC 
# MAGIC This directive has several key features:
# MAGIC * Must be added to a code cell (Python, Scala, R or SQL)
# MAGIC * In the source notebook there should be no modification to the content, facilitating testing of all the solutions and the source notebook as a whole
# MAGIC * When published this directive affects two notebooks:
# MAGIC     * The Student Notebook: added to the same relative path as this notebook however, the corresponding command will be excluded from this notebook entirely.
# MAGIC     * The Solutions Notebook: the published version of this notebook will be republished under the **/Solutions** folder but in the case of this directive, the corresponding command will be included.

# COMMAND ----------

# ANSWER

print("Hello World")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The TODO and ANSWER Directives
# MAGIC This pair of directives has several key features:
# MAGIC * There must be at least one ANSWER cell for every TODO cell - that is to say a solution must be provided
# MAGIC * There can be more ANSWER cells than TODO cells.
# MAGIC * This pari of directives is expected to appear in labs only
# MAGIC * This pair of directives should never appear in the main body of a lesson as it distracts from the instruction process.
# MAGIC * If included in the main body of a lesson, there should be no dependency in subsequent cells. If a student is unable to complete this one TODO exercise, it can preclude them from finishing the lesson as a whole.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
