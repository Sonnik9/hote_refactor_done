import sys
import time
import math

def connnector():
    try:
        import time
        import mysql.connector
        from mysql.connector import connect, Error 
        import config_real
        print('hello db_writerr')

        config = {
            'user': config_real.user,
            'password': config_real.password,
            'host': config_real.host,
            'port': config_real.port,
            'database': config_real.database,      
        }
        for _ in range(3):
            try:
                conn = mysql.connector.connect(**config)      
                print("Writerr connection established refactor")
                break
            except Error as e:
                print(f"Error connecting to MySQL: {e}")
                time.sleep(3)
                continue            
        try:
            cursor = conn.cursor() 
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
    except Exception as ex:
        print(f"30_writerr__{ex}")

    return conn, cursor

def db_wrtr():
    from mysql.connector import connect, Error 
    try:
        conn, cursor = connnector()
    except:
        pass

    try:        
        update_done(conn, cursor)
    except Exception as ex:
        print(f"45_writerr__{ex}")                 

    try:
        cursor.close()
        conn.close()
    except Error as e:
        print(f"Error connecting to MySQL: {e}")   

    return print("refactor upz_hotels 'done' was done successful!")


def update_done(cursor, conn):
    print("update_done start")
    try:
        update_query = "UPDATE upz_hotels SET done = %s WHERE id = %s"
        batch_size = 400
        batch_values = []

        for i in range(326859):
            value = (1, i+1)
            batch_values.append(value)

            if len(batch_values) >= batch_size:
                try:
                    for _ in range(5):
                        try:                        
                            cursor.executemany(update_query, batch_values)
                            conn.commit()
                            batch_values = []
                            print(f"Success batch__{i+1}")
                            break
                        except:
                            try:
                                conn, cursor = connnector()
                                continue
                            except:
                                pass

                except Exception as ex:
                    print(f"Error executing update query: {ex}")
                 
                    batch_values = []

        if batch_values:
            try:
                for _ in range(5):
                    try:                        
                        cursor.executemany(update_query, batch_values)
                        conn.commit()
                        batch_values = []
                        print(f"Success batch__0")
                        break
                    except:
                        try:
                            conn, cursor = connnector()
                            continue
                        except:
                            pass

            except Exception as ex:
                print(f"Error executing update query: {ex}")
                  

        print("Update completed successfully.")

    except Exception as ex:
        print(f"Error executing update query: {ex}")

    return


def main():
    db_wrtr()

if __name__ == "__main__":
    start_time = time.time() 
    main() 
    finish_time = time.time() - start_time
    print(f"Total time:  {math.ceil(finish_time)} сек")
    sys.exit()
   




        