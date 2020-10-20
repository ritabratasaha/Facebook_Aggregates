import json

str = "[{'value': {'Other': 1, 'Your Page': 3} }]"

txt_val = str.replace("\'", "\"")
str1_list = json.loads(txt_val)

str1_dict = str1_list[0]["value"]

for key, value in str1_dict.items():
        print(key,value)



sumofvalues = 0
for key, value in str1_dict.items():
    sumofvalues = sumofvalues + value 
print("Sum of keys = {0} ".format(sumofvalues))