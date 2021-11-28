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
- Line 1 --> Selected Atributes (S): seperated by spaces and / or commas (EXAMPLE: cust, 1_sum_quant, 2_sum_quant, 3_sum_quant)
- Line 2 --> Number of Gouping Variables (n)
- Line 3 --> Grouping Attributes (V): variables for the "GROUP BY" clause
- Line 4 --> F-Vect (F): format --> Group#_AggregateFunction_TableVariable (EXAMPLE: 1_sum_quant, 1_avg_quant)
- Line 5 --> Select Condition-Vect (sig)
- Line 6 --> Having Condition
- Line 7 --> Query: OPTIONAL, used to produce a table to check against output of generated query, MUST be input on a single line
