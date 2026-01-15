from pyspark.sql.functions import  col, when, sum 
from config.config import results_logger, error_logger


def get_queri_results(ds: dict):

    try:
        # Load tables
        city = ds['city']
        address = ds['address']
        customer = ds['customer']
        
        # Join and aggregate
        result = (city
            .join(address, city.city_id == address.city_id)
            .join(customer, address.address_id == customer.address_id)
            .groupBy("city")
            .agg(
                sum(when(col("active") == 1, 1).otherwise(0)).alias("active_customer"),
                sum(when(col("active") == 0, 1).otherwise(0)).alias("not_active_customer")
            )
            .orderBy(col("not_active_customer").desc())
        )

        results_logger.info("Query 6 executed successfully.")


        # Show results
        result.show()
    except Exception as e:
        error_logger.error(f"Error in Query 6: {str(e)}", exc_info=True)
