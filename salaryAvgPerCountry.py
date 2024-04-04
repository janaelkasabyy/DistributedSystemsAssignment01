# Libraries:
from mrjob.job import MRJob
import statistics
import pandas as pd

#Class implementation:
class AverageSalaryPerCountry(MRJob):

    def mapper(self, _, line): # The mapper method is used to extract the country and salary from the file that i'll specify in the command line.
        year, job_title, salary, company_location = line.split(',') # Splitting the lines by the commas.
        country = company_location.strip().split()[-1] # Extracting the country from the company location
        # Yielding the country and salary as a key-value pair <country, salary>:
        yield country, float(salary)

    def reducer(self, country, salaries): # The reducer method is used to calculate the average salary for each country.
        avg_salary = statistics.mean(salaries) # Calculating the average salary for each country.
        # Yielding each country and its average salary
        yield country, avg_salary

if __name__ == '__main__': # To run the MRJob class
    AverageSalaryPerCountry.run()
    # Now, i'll extract from the output_task1.txt file the highest paying country:
    df = pd.read_csv('output_task2.txt', delimiter='\t', header=None, names=['Country', 'Average_Salary']) # Reading the output file
    highest_paying_country = df[df['Average_Salary'] == df['Average_Salary'].max()] # Extracting the highest paying country
    print("Highest Paying Country:") # Printing the highest paying country
    print(highest_paying_country)