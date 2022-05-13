
#Created 3 folders accordingly, /data_source1 , /data_source2, output
#with /data_srouce1 having sample_data1.csv, sample_data2.dat
#with /data_srouce2 having matrial_reference.csv, sample_data3.dat

#importing necessary lib
import pandas as pd
import glob, os, shutil, re, csv

#setting the present working directory, similarly like pwd in unix
os.chdir("C:/Users/cn283906/OneDrive - Centene Corporation/Desktop/Python/int_task/Input/data_source_2")
# file_ext = glob('*.txt') + glob('*.dat')

#I am trying to find any files other than csv in /data folder2 (in this case, dat/txt and move to the source folder1 by using regex and shutil)
for x in os.listdir('.'):
  if re.match('.*\.(txt|dat)', x):

    shutil.move(x, 'C:/Users/cn283906/OneDrive - Centene Corporation/Desktop/Python/int_task/Input/data_source_1')


os.chdir("C:/Users/cn283906/OneDrive - Centene Corporation/Desktop/Python/int_task/Input/data_source_1")

parent_dir = 'C:/Users/cn283906/OneDrive - Centene Corporation/Desktop/Python/int_task/Input/data_source_1'

#finding the non-csv files in the /data source 1 folder and seggreating those file delimters categorically by using sniffer and converting all of them to csv

for x in os.listdir('.'):
    if re.match('.*\.(txt|dat)', x):
      with open(x, newline='') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read())

        if dialect.delimiter == ',':

                df = pd.read_csv(x, sep=',', header=None)
                df.to_csv(x + ".csv", header=None, index=False)
                #print('comma')

        elif dialect.delimiter == ';':

                df = pd.read_csv(dat_file, sep=';', header=None)
                df.to_csv(dat_file + ".csv", header=None, index=False)
                # print('semicolon')

        elif dialect.delimiter == '|':

                df = pd.read_csv(x, sep='|', header=None)
                df.to_csv(x + ".csv", header=None, index=False)
                #print('pipe')

#next task is to find the filenames of all those csv files and store them as "source_file" column in "data" dataframe

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

#Thank you again for the oppurunity mo :) , brings me back to learning mode and hang of the possible exceptions

#The reason why i have used the try catch here is above are dat and txt file and read_csv doesnt nothing to them, if its a faulty .dat,.txt, its jus ignoring 

# i have used alot of test csv data files of my own scenarios, one without data, one with improper format, with data before headers etc in the folders and did some #thorough testing and executed, then i have listed only Parsererror, EmptyDataerror, because those are some possible scenarios here, if there is any impropr headers #or data inlcuded before the headers and also if the csv file is empty then it catches and displays the proper error message with the respective filename.
#there are other usual errors like Filenotfound ( which shows up, when we are reading a specific file and if its not found, but i didnt use it here as its not #applicable here as we are doing a regex and searching and teling it to dont do anything if not found scenario) and PerformanceWarning might occur in scenarios #where its taking too much time because of to much processing of files, then by the exceptin, we can supress it too,this might occur in your proj in live prod #sceanrios

try:
  file_list = []
  for f in all_filenames:
    data = pd.read_csv(f)
    data['source_file'] = f  # create a column with the name of the file
    file_list.append(data)

except pd.errors.EmptyDataError:
    print(f, 'has No data.')
except pd.errors.ParserError:
    print(f, 'has incorrect data format.')
  
    
#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
combined_filename = pd.concat(file_list, axis=0, ignore_index=True)

#converting dataframe to csv and overwriting and adding new column "source_file" to the exisitng csv named "consolidated_output.1.csv" in the /output folder

os.chdir("C:/Users/cn283906/OneDrive - Centene Corporation/Desktop/Python/int_task/Output")
combined_csv.to_csv( "consolidated_output.1.csv", index=False, encoding='utf-8-sig')
combined_filename.to_csv( "consolidated_output.1.csv", index=False, encoding='utf-8-sig')


df = pd.read_csv('consolidated_output.1.csv')

#BONUS questions1 - Dropping all the record which has worth < 1.00 and source_file = sample_Data.1
df.drop(  df[(df['worth']  < 1.00) & (df['source_file'] == 'sample_data.1.csv')].index, inplace=True)

#BONUS questions2 - Caluculating the actual worth of the meterila by using divison arthematic operation
df.loc[df["source_file"] == "sample_data.3.dat.csv", "worth"] = df["worth"]/df["material_id"]


#Finally saving the file
df.to_csv( "consolidated_output.1.csv", index=False, encoding='utf-8-sig')


#**********************************Note - I will be honest that, I am well aware that although I have achived the task , it is without using any user defined functions and passing params,
# which you may be expecting, but with the python experience i have in this Data Engg field, I have achived the above task with the knowledge of python i've  acquired over the years in my workplace
# and i dont thinking learning more is a hardship for me.
# Thanks, TJ :) ***********************

