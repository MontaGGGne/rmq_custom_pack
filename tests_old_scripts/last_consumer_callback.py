def __callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)

    logging.info(f"Consumer callback body: {body}")
    print(f"Consumer callback body: {body}")

    body_dict = {}
    body_dict = json.loads(body)
    unit_number = body_dict["unit number"]
    time_in_cycles = body_dict["time in cycles"]

    current_time = time.time()
    local_time = time.localtime(current_time)
    current_time_str = time.strftime('%Y-%m-%d %H:%M:%S', local_time)
    # cur_day = local_time.tm_mday
    # cur_mounth = local_time.tm_mon
    # cur_year = local_time.tm_year
    # cur_hours = local_time.tm_hour
    # cur_minuts = local_time.tm_min

    base_dir = "units"
    current_time_dir = f"{current_time_str}"
    current_unit = f"unit_number_{unit_number}"
    current_filename = f"time_in_cycles_{time_in_cycles}.json"

    base_dir_prefix = self.__s3_connection.list_objects_v2(Bucket='nasa-turbofans',
                                                            Prefix=f"{base_dir}/",
                                                            Delimiter = "/",
                                                            MaxKeys=1000)
    full_path: str = ''
    if 'CommonPrefixes' not in base_dir_prefix:
        full_path = os.path.join(base_dir, current_time_dir, current_unit, current_filename)
        self.__s3_connection.put_object(Bucket=bucket, Key=full_path, Body=json.dumps(body_dict))
    else:
        last_prefix = base_dir_prefix['CommonPrefixes'][-1]['Prefix'].rstrip('/')
        last_time_dir = last_prefix.split('/')[-1]
        last_time = datetime.strptime(last_time_dir, '%Y-%m-%d %H:%M:%S').timestamp()
        # dir_name_as_list = last_time_dir.split('_')
        # dir_date = dir_name_as_list[0].split('-')
        # dir_time = dir_name_as_list[1].split('-')
        # if cur_day==int(dir_date[0]) and cur_mounth==int(dir_date[1]) and cur_year==int(dir_date[2]) and cur_hours==int(dir_time[0]) and abs(cur_minuts-int(dir_time[1]))<10:
        if abs(last_time - current_time) <= timeout_sec:
            full_path = os.path.join(last_prefix, current_unit, current_filename)
            self.__s3_connection.put_object(Bucket=bucket, Key=full_path, Body=json.dumps(body_dict))
        else:
            full_path = os.path.join(base_dir, current_time_dir, current_unit, current_filename)
            self.__s3_connection.put_object(Bucket=bucket, Key=full_path, Body=json.dumps(body_dict))

    ch.basic_publish(exchange=self.__exchange,
                        routing_key=self.__r_key_response,
                        body=full_path)