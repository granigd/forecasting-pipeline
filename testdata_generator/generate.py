import sys
import pandas as pd
import math
import random


### config ###
path_original = "./original"
path_generated = "./generated"
original_master_data = "master_data.csv"
original_movement_data = "movement_data.csv"

# specifies the increase in warehouses 0.5 = 50% increase
# the higher this number, the less products will be in each warehouse
increase_warehouses_by = 0.5


if __name__ == "__main__":
    args = sys.argv
    if args and len(args) > 1:
        size = int(args[1])
        df_master = pd.read_csv(path_original + "/" + original_master_data)
        df_movement = pd.read_csv(path_original + "/" + original_movement_data)

        original_length = len(df_master.index)
        num_warehouses = len(df_master["warehouse_id"].value_counts())
        max_warehouse_id = 0
        for x in df_master['warehouse_id']:
            if int(x.split(" ")[-1]) > max_warehouse_id:
                max_warehouse_id = int(x.split(" ")[-1])

        num_articles = len(df_master["article_id"].value_counts())
        max_article_id = 0
        for x in df_master['article_id']:
            if int(x.split(" ")[-1]) > max_article_id:
                max_article_id = int(x.split(" ")[-1])

        time_series_to_generate = size - original_length
        num_new_warehouses = int(num_warehouses + (num_warehouses * increase_warehouses_by))
        num_new_articles_per_warehouse = int(math.ceil(time_series_to_generate / num_new_warehouses))

        df_master_generated = df_master.copy()
        df_movement_generated = df_movement.copy()

        original_index = 0
        generated_index = 0
        for i_wh in range(max_warehouse_id + 1, max_warehouse_id + num_new_warehouses):
            for i_art in range(1, num_new_articles_per_warehouse + 1):

                if time_series_to_generate == 0:
                    break

                df_master_generated = df_master_generated.append({
                    "warehouse_id": f"Lager {i_wh}",
                    "article_id": f"Artikel {i_art}",
                    "n_steps": df_master["n_steps"].iloc[original_index],
                    "from": df_master["from"].iloc[original_index],
                    "to": df_master["to"].iloc[original_index],
                }, ignore_index=True)

                base_wh = df_master["warehouse_id"].iloc[original_index]
                base_art = df_master["article_id"].iloc[original_index]
                base_ts = df_movement[(df_movement["warehouse_id"]==base_wh) & (df_movement["article_id"]==base_art)].copy()
                base_ts.quantity = base_ts.quantity * random.randint(1, 50)
                base_ts.warehouse_id = f"Lager {i_wh}"
                base_ts.article_id = f"Artikel {i_art}"
                df_movement_generated = df_movement_generated.append(base_ts)

                original_index += 1
                if original_index >= original_length:
                    original_index = 0
                time_series_to_generate -= 1
            else:
                print(f"Generated warehouse {i_wh} from {max_warehouse_id + num_new_warehouses}")
                continue
            break

        df_master_generated.to_csv(path_generated + "/" + original_master_data.replace(".csv", "_" + str(size) + ".csv"), index=False)
        df_movement_generated.to_csv(path_generated + "/" + original_movement_data.replace(".csv", "_" + str(size) + ".csv"), index=False)
