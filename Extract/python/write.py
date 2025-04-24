def write_to_nas(df, output_format, file_path, logger, header=False):
    try:
        logger.info(f"Starting write process ... ")
        df.write.format (output_format).mode("overwrite").options(header=header).save(file_path)
        logger.info(f"Data written to {file_path}")
    except Exception as e:
        logger.error(f"Error writing data to NAS at {file_path}: {e}")

def write_text_file(file_path, content, logger):
    try:
        with open(file_path, "w") as file:
            file.write(content)
    except Exception as e:
        logger.error(f"Error writing data to Nas at {file_path}: {e}")