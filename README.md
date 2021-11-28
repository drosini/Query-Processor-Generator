# Query-Processor-Generator
Program that generates a Python script that can process a user inputted Extended Multi-Feature Query


Requirements:

- PostGres Server with "SALES" table
- Queries broken down into relational algebra statements

Instructions:

1) Initliaize instance of queryProcessor()
2) Enter PostGres access information
3) Call "passArguements()" (either enter query manually or pass file)
4) Call "buildQuerry()"



Query Arguements File Paramters:

- File Type: .txt
- Line 1 
-----> Selected Atributes (S) 
-----> seperated by spaces and / or commas
-----> e.x. cust, 1_sum_quant, 2_sum_quant, 3_sum_quant
- Line 2 --> Number of Gouping Variables (n)
- Line 3 --> Grouping Attributes
