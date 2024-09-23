Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


### Analysis

Analysis folder contains SQL queries for Part I of the exercise.

### Raw Data
In seed folder I've included 3 csv with dummy data used to run the models.

### Models

1. Staging
We have first layer with staging models - 1 for each source table. Basic transformations could be performed here. One assumption about delayed period is implemented here - simply that there is a specific bucket for orders with delayed period equal to 35 days. The alternative approach would be to include the order twice - once for each of the two buckets. This could be achieved with a JOIN statement in the code.

2. Intermediate
In the intermediate phase we split the orders in two models - orders in arrears and orders up to date with payments. Materialization ia a view so an order will move from one model to another in the moment there are overdue amounts (when is_in_default=true). Since we would be interested in the last up to 6 months of data (assumption: people have up to 3 months to pay the loan), performance should not be an issue and implementing incremental logic would not be recommended. Additional fields from dimension tables are added as well.

Separating the orders in two models would also allow to use the orders separately for other reports and analysis.

3. Marts: Reporting table
Finally, in the marts/finance folder we have our final model - loan_default, where aggregation and calculations are made. Metrics used to calculate the default ratio are kept in the model in order to allow for more fine grained analysis.

### Tests

Out of the box dbd tests are included, as well as a singular test (with generic version implemented as a macro).

### Documentation

Run the following command to see the documentation in your browser:

```
dbt docs generate
dbt docs serve
``` 

