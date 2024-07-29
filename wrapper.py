import dask.distributed as dask_dt 
import compute_utils.parse_file  as prs 
import compute_utils.cluster_init  as clstr
import compute_utils.review_transform as rvw_trs
import fs_utils.file_utils as  fs
import logs_utils.dask_logger as dsk_logger 
import sys
import time 
import os 

def main():
    try:
        # get the arguments from environment variables
        review_file_path = os.environ.get('input')
        inappropriate_words_file_path =  os.environ.get('inappropriate_words')
        output_file_path= os.environ.get('output')
        aggregations_file_path =  os.environ.get('aggregations')


        # intialize the root logger
        logger = dsk_logger.setup_logging('dask_root_logger')

        # logger
        logger.info(f"********** Starting the Review Aggregation Batch Pipeline **********")
        logger.info(f"Args: {dict(review_file_path=review_file_path, inappropriate_words_file_path=inappropriate_words_file_path, output_file_path=output_file_path, aggregations_file_path=aggregations_file_path)}")
        logger.info("Creating Dask Cluster and Client")

        # step 1  create dask cluster
        local_cluster = clstr.create_local_dask_cluster()

        # logger
        logger.info(f"Local Cluster created with configs : {local_cluster}")

        # step 2 create dask client  with local cluster
        with dask_dt.Client(local_cluster) as client:

            # logger
            logger.info(f"****************************************************************************************")
            logger.info(f"Created Dask Client with configs : {client}, Dashboard Link : {client.dashboard_link}")
            logger.info(f"****************************************************************************************")
            
            # step 3 register logger function to client
            client.run(dsk_logger.setup_logging)

            # step 4.1  get the all  the files
            review_files = fs.get_all_objects(review_file_path)

            # logger
            logger.info(f"Got all the review files : {review_files}")

            # step 4.2 get  latest file to parse 
            latest_review_file_path = fs.get_latest_files(review_file_path)

            if len(latest_review_file_path)==0:
                logger.info(f"No review file found in {review_file_path}")
                sys.exit(0)

            #logger
            logger.info(f"Got the latest review file : {latest_review_file_path}")

            # logger 
            logger.info(f"Started Parsing the review files ")
            
            # parse  &  validate the review data
            valid_ddf_fut,invalid_ddf_fut = prs.process_json(client,latest_review_file_path,logger)

            # process , apply text masking ,filter the inappropriate words and aggregate the reviews
            processed_rev_df = rvw_trs.process_reviews(client,valid_ddf_fut,inappropriate_words_file_path,output_file_path,aggregations_file_path,logger)

            # filter the invalid data vs unparseable files
            invalid_ddf = invalid_ddf_fut

            # logger
            logger.info(f"Writing the invalid data to error directory")

            # get the invalid data and dump into error directory
            invalid_ddf[invalid_ddf.isnull().any(axis=1)].to_json(f"{output_file_path}/error/invalid_data_{time.time()}.json",orient='records',lines=True)

            # logger
            logger.info(f"Writing Processed Files to Output Directory")
            processed_rev_df.to_json(f"{output_file_path}/processed_review/processed_data_{time.time()}.json",orient='records',lines=True)

            # move the processed file to processed directory
            fs.move_processed_files(review_files,f"{output_file_path}/processed_file")

            # logger
            logger.info(f"Moved the processed files to processed directory")

            # pause the execution for 10 seconds , hold the client and cluster
            time.sleep(10)

            # close the client
            client.close()

            # close the cluster
            local_cluster.close()

            logger.info(f"List of files processed successfully As part of the Review Aggregation Batch Pipeline :{review_files}")
            logger.info(f"********** Completed the Review Aggregation Batch Pipeline **********")

    except Exception as e:
        # close the client
        client.close()
        
        # close the cluster
        local_cluster.close()

        # raise  exception 
        raise e


if __name__=='__main__':
    main()