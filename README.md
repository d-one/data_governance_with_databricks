# Basic Data Governance with Databricks
Repository for the D ONE blogpost for basic data governance using Databricks

Medium post link : 

# How to use the content

In order to be able to recreate the example you need to have modification rights for a catalog, please contact your admin if you don't. Alternatively, see the following documentation if you need to create a new one: https://docs.databricks.com/en/data-governance/unity-catalog/create-catalogs.html

In the materials below the catalog is assumed to be named "store_data_catalog". Make sure you change it to the name of the catalog you are planning to work with.

Steps:
 * Downlod the DBC archive file from this repo
 * Upload it to your workspace in Databricks via import file
 * If needed - change the name of the catalog in the notebook (mind, there are multiple locations) and the sql queries
 * Run the Notebook
 * Review the result in the catalog. It should looke similar to the following image:
 ![Expected Catalog](./images/expected_catalog.png)
 * Run the data quality queries
 * Configure visualisations and create a data quality dashboard (see separate instructions below)

 Configuring visualisations:
1. Detailed health check report:
Navigate to the query "blog_post_generating_dq_report". After execution there will appear a tab called "Results" at the bottom. Double click on the name "Results" and change it to something meaningful, such as "Full data quality check report". Click on the arrow next to the tab name and choose edit as indicated by the following image:
![Configure Report p1](./images/configure_table_p1.png)
A window should open with options to configure the visualisation. In this case we are interested in a detailed, tabular report, therefore, make sure the upper left corner shows "Table" under "Visualization type":
![Configure Report p2](./images/configure_table_p2.png)
To draw attention to failed checks we would like to colour several attributes in orange. Click on each of the following attributes: "Check", "Data Element" and "Check Status" and add a condition as shown in the following image: 
![Configure Report p3](./images/configure_table_p3.png)
After clicking on "Save" at the bottom right corner navigate back to query, and you should the result at the bottom looking similar to the following image (mind, the exact values may be different):
![Configure Report p4](./images/configure_table_p4.png)
 2. Summary doughnut charts for data quality categories: Navigate to the query "blog_post_dq_category_summary". Similarly to the steps in (1) rename the "Results" to something meaningful, such as "dq_summary_category". In the "Edit" pop-up choose "Pie" under "Visualization type" and set the X column to "Check Status" and the Y to Count (Sum). Group by the "check_type". Your configuration should be similar to this:
 ![Configure Summary p1](./images/configuredoughnut_p1.png)
 To set the colours navigate to the "Colors" tab and click on the colour to change it to the ones presented in the image below:
![Configure Summary p2](./images/configuredoughnut_p2.png)
Your doughnut charts should look like in the previous image.
3. General information regarding data checks execution:
  
