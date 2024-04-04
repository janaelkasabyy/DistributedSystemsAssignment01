# Libraries:
from mrjob.job import MRJob
import statistics
import pandas as pd

# Class implementation:
class AverageSalaryPerJob(MRJob):

    def mapper(self, _, line): # The mapper method is used to extract the job title and salary from the file that i'll specify in the command line.
        year, job_title, salary, company_location = line.split(',') # Splitting the lines by the commas.
        # Yielding the job title and salary as a key-value pair <job_title, salary>:
        yield job_title.strip(), float(salary) #.strip() removes leading and trailing whitespaces

    def reducer(self, job_title, salaries): # The reducer method is used to calculate the average salary for each job title.
        avg_salary = statistics.mean(salaries) # Calculating the average salary for each job title.
        # Yielding each job title and its average salary
        yield job_title, avg_salary

if __name__ == '__main__': # To run the MRJob class
    AverageSalaryPerJob.run()
    # Now, i'll extract from the output_task1.txt file the highest paying job:
    df = pd.read_csv('output_task1.txt', delimiter='\t', header=None, names=['Job_Title', 'Average_Salary']) # Reading the output file
    highest_paying_job = df[df['Average_Salary'] == df['Average_Salary'].max()] # Extracting the highest paying job
    print("Highest Paying Job:") # Printing the highest paying job
    print(highest_paying_job)