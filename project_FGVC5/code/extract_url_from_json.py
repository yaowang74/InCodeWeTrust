
# coding: utf-8

# In[1]:

"""A script to handle kaggle FGVC5 json files

   moduleauthor:: InCodeWeTrust

"""

import requests
import time
import json
import pandas as pd
# from PIL import Image
# from io import BytesIO

JSON_FILE_PATH = "/home/wyao/github/InCodeWeTrust/project_FGVC5/data/input/"
IMAGE_PATH = "/home/wyao/github/InCodeWeTrust/project_FGVC5/data/working/"
TRAIN_FILE_NAME = "train.json"
TEST_FILE_NAME = "test.json"
VALI_FILE_NAME = "validation.json"
MAX_RETRY = 3

train_image_file = JSON_FILE_PATH + TRAIN_FILE_NAME
# print(image_url_file)


# In[2]:

def parse_json(json_file, json_key):
    """This function extracts image urls from json file.
    
    :param json_file: the full path of json file
    :type name: str
    :param json_key: component to be extract from input json file. 
                Possible values are ['images','annotation'].
    :type json_key: str
    :returns: pandas data frame with image id and url.
    
    """
    with open(train_image_file, 'r') as f:
        image_dict = json.load(f)
    print(image_dict.keys())

    if (json_key not in ['images','annotations']):
        raise ValueError("Invalid input argument: json_key.")
    if (json_key == 'images'):
        image_url_df = pd.DataFrame(image_dict['images'])
        image_url_df.url = image_url_df.url.apply(lambda x: x[0])
        print(image_url_df.head())
        return image_url_df
    elif (json_key == 'annotations'):
        image_annotation_df = pd.DataFrame(image_dict['annotations'])
        print(image_annotation_df.head())
        return image_annotation_df


# In[3]:

image_url_df = parse_json(train_image_file, 'images')
image_annotations_df = parse_json(train_image_file, 'annotations')
image_annotations_df.to_csv(JSON_FILE_PATH + "train_label.csv",
                            index=False)


# In[4]:

def show_image_by_id(image_dataframe, image_id, save = False):
    """This function show image by id.
    
    :param image_dataframe: a data frame contains image id and image url.
    :type image_dataframe: pandas data frame
    :param image_id: a positive integer that represents image id
    :type image_id: integer
    :parame save: boolean argument indicating if the image will be saved
    :type save: boolean
    :returns: image object
    
    """
    image_index = image_id - 1

    url = image_dataframe.iloc[image_index]['url']
    if url.find('/'):
        original_image_name = url.rsplit('/', 1)[1]
        image_data = requests.get(url).content
        new_image_name = str(image_id) + '.jpg'
        new_image_path = IMAGE_PATH + new_image_name
        if (save):
            with open(new_image_path, 'wb') as handler:
                handler.write(image_data)
            print("Saved image: {} as {}.".format(original_image_name,
                                                  new_image_name))
        else:
            img = Image.open(BytesIO(image_data))
            img.show()
    else:
        print("Image is not found.")

show_image_by_id(image_url_df, 4, True)


# In[8]:

def save_image_batch(image_dataframe, dest, id_start_from=1):
    """This function show image in batch.
    
    :param image_dataframe: a data frame contains image id and image url.
    :type image_dataframe: pandas data frame
    :param dest: dest possible values: ["train","test","validation"]
    :type dest: str
    :param id_start_from: image id starts from, has to be positive integer.
    :type id_start_from: int
    :returns: 
    
    """
    num_image = image_dataframe.shape[0]
    if dest == "train":
        saving_path = IMAGE_PATH + "train/"
    elif dest == "test":
        saving_path = IMAGE_PATH + "test/"
    elif dest == "validation":
        saving_path = IMAGE_PATH + "validation/"
    else:
        raise ValueError("invalid destination arg.")

    for idx in range(id_start_from-1, num_image):
        image_id = idx + 1
        url = image_dataframe.iloc[idx]['url']

        original_image_name = url.rsplit('/', 1)[1]
        retry = 0
        image_retrieved = True
        while retry < MAX_RETRY:
            try:
                image_data = requests.get(url,timeout=10).content
                break
            except:
                retry += 1
                print("Connection refused by the server..")
                print("Let me sleep for 5 seconds")
                print("ZZzzzz...")
                time.sleep(5)
                print("Was a nice sleep, now let me continue...")
                if retry == MAX_RETRY:
                    image_retrieved = False
                    print("Failed to get {}.jpg".format(image_id))
                continue

        if image_retrieved == True:
            new_image_name = str(image_id) + '.jpg'
            new_image_path = saving_path + new_image_name

            with open(new_image_path, 'wb') as handler:
                handler.write(image_data)
            print("Saved image: {} as {}.".format(original_image_name,
                                                  new_image_name))


# In[7]:

# save training image
save_image_batch(image_url_df,"train",293)

