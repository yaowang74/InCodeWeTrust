# -*- coding: utf-8 -*-
"""
Spyder Editor

Author: InCodeWeTrust
"""

import requests
import time
import json
import pandas as pd
from multiprocessing import Pool
#from PIL import Image
#from io import BytesIO

JSON_FILE_PATH = "/home/wyao/github/InCodeWeTrust/project_FGVC5/data/input/"
IMAGE_PATH = "/home/wyao/github/InCodeWeTrust/project_FGVC5/data/working/"
TRAIN_FILE_NAME = "train.json"
TEST_FILE_NAME = "test.json"
VALID_FILE_NAME = "validation.json"
MAX_RETRY = 3
TIME_OUT = 5

train_image_file = JSON_FILE_PATH + TRAIN_FILE_NAME
valid_image_file = JSON_FILE_PATH + VALID_FILE_NAME
test_image_file = JSON_FILE_PATH + TEST_FILE_NAME

#%%
def parse_json(json_file, json_key):
    """This function extracts image urls from json file.
    
    :param json_file: the full path of json file
    :type name: str
    :param json_key: component to be extract from input json file. 
                Possible values are ['images','annotation'].
    :type json_key: str
    :returns: pandas data frame with image id and url.
    
    """
    with open(json_file, 'r') as f:
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

#%%
def download_image(dest, image_id, image_url):
    """This function save image in batch with multiprocessing.
    
    :param image_id: image id.
    :type image_id_list: int
    :param image_url: image url.
    :type image_url: str    
    :param dest: dest possible values: ["train","test","validation"]
    :type dest: str
    :returns:
    
    """
    if dest == "train":
        saving_path = IMAGE_PATH + "train/"
    elif dest == "test":
        saving_path = IMAGE_PATH + "test/"
    elif dest == "validation":
        saving_path = IMAGE_PATH + "validation/"
    else:
        raise ValueError("Invalid destination argument.")

#    original_image_name = image_url.rsplit('/', 1)[1]
    retry = 0
    image_retrieved = True
    while retry < MAX_RETRY:
        try:
            image_data = requests.get(image_url,timeout=TIME_OUT).content
            break
        except:
            retry += 1
#            print("Connection refused by the server...wait for 5 seconds")
            time.sleep(5)
            if retry == MAX_RETRY:
                image_retrieved = False
                print("Failed to get {}.jpg".format(image_id))
            continue

    if image_retrieved == True:
        new_image_name = str(image_id) + '.jpg'
        new_image_path = saving_path + new_image_name
        with open(new_image_path, 'wb') as handler:
            handler.write(image_data)


def save_image_parallel(image_dataframe, dest):
    """This function save image in batch in parallel.
    
    :param image_dataframe: a data frame contains image id and image url.
    :type image_dataframe: pandas data frame
    :param dest: dest possible values: ["train","test","validation"]
    :type dest: str
    :returns:
    """
    image_url_list = image_dataframe['url'].tolist()
    # keep constant number of process
    process = Pool(5)
    job_args = [(dest, index+1, image_url_list[index]) for index, url in enumerate(image_url_list)]

    process.starmap(download_image, job_args)

# %%

image_url_df = parse_json(train_image_file, 'images')
image_url_list = image_url_df['url'].tolist()
save_image_parallel(image_url_df, "train")

# %%

image_url_df = parse_json(valid_image_file, 'images')
image_url_list = image_url_df['url'].tolist()
save_image_parallel(image_url_df, "validation")

# %%

image_url_df = parse_json(test_image_file, 'images')
image_url_list = image_url_df['url'].tolist()
save_image_parallel(image_url_df, "test")

#%%
# No need to use this function as no parallelism implememnted here
def save_image_serial(image_dataframe, dest, id_start_from=1):
    """This function save image in batch, not in parallel manner.
    
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
        raise ValueError("Invalid destination argument.")

    if (not isinstance(id_start_from, int)) | (id_start_from < 1):
        raise ValueError("Invalid image id argument.")

    for idx in range(id_start_from-1, num_image):
        image_id = idx + 1
        url = image_dataframe.iloc[idx]['url']

#        original_image_name = url.rsplit('/', 1)[1]
        retry = 0
        image_retrieved = True
        while retry < MAX_RETRY:
            try:
                image_data = requests.get(url,timeout=10).content
                break
            except:
                retry += 1
#                 print("Connection refused by the server...wait for 5 seconds")
                time.sleep(5)
                if retry == MAX_RETRY:
                    image_retrieved = False
                    print("Failed to get {}.jpg".format(image_id))
                continue

        if image_retrieved == True:
            new_image_name = str(image_id) + '.jpg'
            new_image_path = saving_path + new_image_name

            with open(new_image_path, 'wb') as handler:
                handler.write(image_data)
#             print("Saved image: {} as {}.".format(original_image_name,
#                                                   new_image_name))
    print("DONE!")
#%%

#save_image_serial(image_url_df,"train",24941)

