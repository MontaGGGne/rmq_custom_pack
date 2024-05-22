def __callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)

    body_dict = {}
    body_dict = json.loads(body)
    unit_number = body_dict["unit number"]
    time_in_cycles = body_dict["time in cycles"]

    # current_dir = os.path.join(SCRIPT_DIR, f"unit_number_{unit_number}")
    # if Path(current_dir).exists() is False:
    #     os.mkdir(current_dir)
    #     with open(os.path.join(current_dir, f"time_in_cycles_{time_in_cycles}.json"), 'w') as f:
    #         json.dump(body_dict, f)
    # else:
    #     with open(os.path.join(current_dir, f"time_in_cycles_{time_in_cycles}.json"), 'w') as f:
    #         json.dump(body_dict, f)

    current_dir = f"unit_number_{unit_number}"
    current_filename = f"time_in_cycles_{time_in_cycles}.json"
    self.__s3_connection.put_object(Bucket='nasa-turbofans', Key=f"{current_dir}/{current_filename}", Body=json.dumps(body_dict))

    ch.basic_publish(exchange=self.__exchange,
                     routing_key=self.__r_key_response,
                     body=body)