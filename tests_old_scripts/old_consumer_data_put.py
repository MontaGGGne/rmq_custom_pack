# all_time_prefix = self.__s3_connection.list_objects_v2(Bucket='nasa-turbofans',
#                                                         Prefix=f"{current_dir}/",
#                                                         Delimiter = "/",
#                                                         MaxKeys=1000)
# str_for_body: str = ''
# if 'CommonPrefixes' not in all_time_prefix:
#     self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(current_time_dir, current_filename), Body=json.dumps(body_dict))
#     str_for_body = os.path.join(current_time_dir, current_filename)
# else:
#     last_prefix = all_time_prefix['CommonPrefixes'][-1]['Prefix'].rstrip('/')
#     time_folder = last_prefix.split('/')[-1]
#     dir_name_as_list = time_folder.split('_')
#     dir_date = dir_name_as_list[0].split('-')
#     dir_time = dir_name_as_list[1].split('-')
#     if cur_day==int(dir_date[0]) and cur_mounth==int(dir_date[1]) and cur_year==int(dir_date[2]) and cur_hours==int(dir_time[0]) and abs(cur_minuts-int(dir_time[1]))<10:
#         self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(last_prefix, current_filename), Body=json.dumps(body_dict))
#         str_for_body = os.path.join(last_prefix, current_filename)
#     else:
#         self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(current_time_dir, current_filename), Body=json.dumps(body_dict))
#         str_for_body = os.path.join(current_time_dir, current_filename)
    # for prefix in all_time_prefix['CommonPrefixes']:
    #     time_folder = prefix.split('/')[1]
    #     dir_name_as_list = time_folder.split('_')
    #     dir_date = dir_name_as_list[0].split('-')
    #     dir_time = dir_name_as_list[1].split('-')
    #     if cur_day==int(dir_date[0]) and cur_mounth==int(dir_date[1]) and cur_year==int(dir_date[2]) and cur_hours==int(dir_time[0]) and abs(cur_minuts-int(dir_time[1]))<5:
    #         self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(time_folder, current_filename), Body=json.dumps(body_dict))
    #     else:
    #         self.__s3_connection.put_object(Bucket=bucket, Key=os.path.join(current_time_dir, current_filename), Body=json.dumps(body_dict))