# Beginner Explanatory Guide: PLATFORM-2875: Build data validation layer for ETL pipeline

> **Task Type**: Product Task  
> **Domain/Focus**: Data Validation, ETL Pipelines, Python Programming

---

## 1. The Goal (In-Depth Beginner Explanation)

### The Core Problem
In the context of our ETL (Extract, Transform, Load) pipeline, we currently face a significant issue: the absence of a data validation layer. This means that when data is extracted from various sources and loaded into our data warehouse, there is no mechanism in place to ensure that the data is accurate and conforms to expected formats. As a result, invalid records—such as those with missing fields, incorrect data types, or values that fall outside acceptable ranges—can make their way into production tables. This can lead to downstream errors, such as broken dashboards or incorrect analytics, which ultimately affect decision-making processes based on this data.

Fixing this problem is crucial because it ensures the integrity and reliability of the data we work with. By implementing a validation layer, we can catch errors early in the data processing pipeline, preventing faulty data from causing larger issues later on. This not only enhances the quality of our data but also builds trust with users who rely on accurate reporting and analytics.

### Jargon Buster (Key Terms Explained)
* **ETL (Extract, Transform, Load)**: This is a process used in data warehousing that involves extracting data from various sources, transforming it into a suitable format, and loading it into a destination database. For example, an ETL pipeline might extract sales data from a CSV file, convert the date formats, and load it into a SQL database for analysis.

* **Data Validation**: This refers to the process of ensuring that data is accurate, complete, and meets specified criteria before it is processed or stored. For instance, validating an email address might involve checking that it contains an "@" symbol and a domain name.

* **Schema**: A schema defines the structure of data, including the types of fields and any constraints on those fields. For example, a schema for a user record might specify that the "age" field must be an integer between 0 and 150, and the "email" field must follow a specific format.

* **Quarantine List**: This is a list where invalid records are stored instead of being discarded. Keeping these records allows for further inspection and correction. For example, if a record fails validation due to a missing field, it would be added to the quarantine list for review rather than being lost.

### Expected Outcome
After implementing the data validation layer, the ETL pipeline should behave as follows:

**Before**: Invalid records can enter the data warehouse, leading to errors in reports and analytics. Users may see incorrect data or experience system failures.

**After**: The validation layer will check each record against the defined schema before it is processed. Invalid records will be identified and stored in a quarantine list with detailed error messages, allowing for correction. Users will receive accurate data, and the integrity of the data warehouse will be maintained.

---

## 2. Related Coding Concepts & Syntax (50% Theory, 50% Practice)

### Concept 1: Data Validation
#### 📘 Theoretical Overview (50%)
* **Why it exists**: Data validation is essential to ensure that the data being processed is correct and usable. Without validation, systems can operate on incorrect data, leading to faulty outputs and decisions. For example, if a financial report is generated using incorrect sales figures, it could mislead stakeholders.

* **Key Mechanisms**: Data validation typically involves checking for required fields, ensuring data types match expected formats, and verifying that values fall within specified ranges. This can be done using conditional statements that evaluate each record against the schema.

#### 💻 Syntax & Practical Examples (50%)
* **Language Syntax**:
  ```python
  def validate_record(record: Dict, schema: Dict) -> Dict:
      errors = []
      for field, rules in schema.items():
          if rules.get('required') and field not in record:
              errors.append({'field': field, 'error_type': 'missing_field', 'message': f'Required field {field} is missing'})
          # Additional checks for type and range would follow...
      return {'valid': len(errors) == 0, 'errors': errors}
  ```

* **Real-World Application**:
  ```python
  schema = {
      "name": {"type": "str", "required": True},
      "age": {"type": "int", "required": True, "min": 0, "max": 150}
  }

  record = {"name": "Alice", "age": 30}
  validation_result = validate_record(record, schema)
  # validation_result would return {'valid': True, 'errors': []}
  ```

---

## 3. Step-by-Step Logic & Walkthrough

1. **Step 1: Locate and Analyze the Target File**
   * Navigate to the `src` folder and open `validator.py`. This is where you will implement the data validation logic.
   * Focus on the `validate_record` method, which is currently incomplete and marked with TODO comments.

2. **Step 2: Input Verification & Validation**
   * Begin by checking for required fields. For each field in the schema that is marked as required, verify if it exists in the incoming record. If a required field is missing, append an error message to the errors list.

3. **Step 3: Core Implementation / Modification**
   * Implement checks for field types. For each field present in both the record and schema, compare the actual type of the value against the expected type defined in the schema. If they do not match, record an error.
   * Next, check for numeric ranges. For fields that have `min` or `max` constraints, ensure that the values fall within these limits. If a value is out of range, log an appropriate error message.
   * Finally, implement the logic for custom validation rules, if any are defined.

4. **Step 4: Output Verification & Testing**
   * After implementing the validation logic, run the unit tests provided in `test_pipeline.py` to ensure that all validation checks are functioning correctly. Verify that the expected outputs match the actual outputs.

---

## 4. Detailed Walkthrough of Test Cases

### Test Case 1: Standard / Success Case
* **Description**: This test checks if a valid record passes all validation checks.
* **Inputs**:
  ```json
  {
      "name": "Alice",
      "age": 30
  }
  ```
* **Step-by-Step Execution Trace**:
  1. The `validate_record` function receives the input record.
  2. It checks for required fields: both "name" and "age" are present.
  3. It verifies the types: "name" is a string and "age" is an integer, matching the schema.
  4. It checks the numeric range for "age": 30 is within the specified range of 0 to 150.
  5. Returns the final result: `{'valid': True, 'errors': []}`.
* **Expected Output**: The validation result indicates that the record is valid.

### Test Case 2: Edge Case / Validation Fail
* **Description**: This test checks how the system handles a record with a missing required field.
* **Inputs**:
  ```json
  {
      "age": 30
  }
  ```
* **Step-by-Step Execution Trace**:
  1. The `validate_record` function receives the input record.
  2. It checks for required fields: "name" is missing.
  3. An error is logged: `{'field': 'name', 'error_type': 'missing_field', 'message': 'Required field name is missing'}`.
  4. The function concludes that the record is invalid.
  5. Returns the final result: `{'valid': False, 'errors': [{'field': 'name', 'error_type': 'missing_field', 'message': 'Required field name is missing'}]}`.
* **Expected Output**: The validation result indicates that the record is invalid, along with the specific error message.