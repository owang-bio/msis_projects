class DataPrep:
    """
    Downloading csv files from the box.
    
    Update the database.
    """    
    def __init__(self, 
                 engineStr: "database connection, for sqlalchemy engine"):
        import pandas as pd
        from sqlalchemy import create_engine
        self.engineStr = engineStr
        self.query_dim_equipment = """
        select *
        from dim_equipment
        ;
        """
        self.query_dim_location = """
        select *
        from dim_location
        ;
        """
        self.query_dim_date = """
        select * 
        from dim_date_calendar
        ;
        """
        engine = create_engine(engineStr)
        self.dim_equipment = pd.read_sql_query(self.query_dim_equipment, 
                                               con = engine, 
                                               coerce_float=False)
        self.dim_location = pd.read_sql_query(self.query_dim_location, 
                                              con = engine, 
                                              coerce_float=False)

    def downloadcsv(self, 
                    authPath: "path to your box config file, a json file", 
                    folderID: "the id of the folder on box drive containing the csv files", 
                    date: "the date taht the csv file was created"):
        """Download the csv file from box."""
        from boxsdk import JWTAuth
        from boxsdk import Client
        import os

        sdk = JWTAuth.from_settings_file(authPath)
        client = Client(sdk)
        items = client.folder(folder_id=folderID).get_items()
        dir = os.getcwd()
        for item in items:
            if date in item.name:
                item_id = item.id
                box_file = client.file(file_id= item_id).get()
                output_file = open(f'{os.getcwd()}/raw_csv/{box_file.name}', 'wb')
                box_file.download_to(output_file)
                output_file.close()

    def updatedb_sql(self,
                     path: "path to direcotry contain all csv files"):
        """
        update the database with stored precedures, reads and update for multiple csv files, if necessary.
        """
        import os
        import re
        import pandas as pd
        from sqlalchemy import create_engine
        import datetime
        
        engine = create_engine(self.engineStr)
        date_lst = []
        files = os.listdir(path)
        for file in files:    
            date = re.findall(r'asset-report-(\d{4}-\d{2}-\d{2})', file)
            if date:
                date_lst.append(date[0])
        date_lst.sort()
        
        # loading data using prestored procedures
        procedures = ['UpdateDates()', 
                      'UpdateLocations()', 
                      'UpdateEquipment()', 
                      'updateFact()'
                      ]
        for date in date_lst:
            for file in files:
                if ("asset-report-" + date) in file:
                    df = pd.read_csv(f'{path}/{file}', index_col=False)
                    df.columns = [i.replace(' ', '_') for i in df.columns]
                    df['Loading_Date'] = date
                    df.to_sql(name = 'stage', con = engine, 
                              if_exists = 'replace', index = True)
                    print(f"Stage table ready for {file}...")

                    with engine.begin() as connection:
                        for p in procedures:
                            time1 = datetime.datetime.now()
                            connection.execute(f"CALL {p};")
                            print(f"{p} finished for {file}! Took {datetime.datetime.now() - time1} time")
                    break

    def update_date(self, 
                    dateStr: "string of the date of that the csv file was created"):
        """update dim_date_calendar dimension table."""
        from datetime import date
        from sqlalchemy import create_engine
        import pandas as pd
        
        engine = create_engine(self.engineStr)        
        date_key = date(int(dateStr[0:4]), int(dateStr[5:7]), int(dateStr[8:]))
        dateDict = {"date_key": [dateStr], 
            "cal_year": [int(dateStr[0:4])], 
            "cal_month": [int(dateStr[5:7])], 
            "cal_week_of_year": [date_key.isocalendar()[1]]
            }
        df = pd.DataFrame(dateDict, 
                  columns=["date_key", "cal_year", 
                           "cal_month", "cal_week_of_year"])
        df.to_sql(name = 'dim_date_calendar',
                  con = engine,
                  if_exists = 'append',
                  index = False)
        self.dim_date = pd.read_sql_query(self.query_dim_date, engine)
    
    def update_location(self, 
                        date: "iso date str, format YYYY-MM-DD",
                        raw_df: "pandas dataFrame created from raw csv"):
        """update dim_location"""
        import pandas as pd
        from sqlalchemy import create_engine
        engine = create_engine(self.engineStr)
        
        # raw_df, get distinct location and building and rename
        df_new = raw_df.loc[:, ['location', 'bldg']].drop_duplicates().copy()
        df_new.columns = ["location_name", "building"]
        df_new['effective_dt'] = date
        df_new['expiration_dt'] = '9999-12-31'
        df_new['location_key'] = [i for i in range(1, len(df_new) + 1)]

        # location_key_new = NaN, locations exipired on this date, set exipration date to date
        df_merge = df_new.merge(self.dim_location.loc[self.dim_location['expiration_dt'].apply(str) == '9999-12-31'], 
                                how = 'outer', 
                                on=['location_name', 'building'],
                                suffixes = ["_new", "_current"])
        expired_records = df_merge[pd.isna(df_merge['location_key_new'])]
        with engine.begin() as connection:
            keys = [int(float(i)) for i in expired_records['location_key_current'].tolist()]
            if len(keys) != 0:
                query = f"""
                UPDATE dim_location
                SET expiration_dt = '{date}'
                WHERE location_key in ({keys});""".replace('[', '').replace(']', '')
                connection.execute(query)
                
        # if only in new raw_df, location_key_current = NaN after outer join; insert them into dim_location
        new_records = (
            df_merge[pd.isna(df_merge['location_key_current'])]
            .loc[:, ['location_name', 
                     'building', 
                     'effective_dt_new', 
                     'expiration_dt_new']]
            .rename(columns={'effective_dt_new': 'effective_dt',
                    'expiration_dt_new': 'expiration_dt'})
                       )
        
        # add location_key, not needed if primary key is auto_increment
        # new_records.insert(loc = 0,
        #             column = 'location_key', 
        #             value = [i for i in range(len(self.dim_location) + 1, 
        #                                       len(self.dim_location) + len(new_records) + 1)])
        
        new_records.to_sql(name = 'dim_location', 
                con = engine, 
                if_exists = 'append', 
                index = False)
        
        self.dim_location = (pd.read_sql_query(self.query_dim_location, 
                                               con = engine, coerce_float=False)
                                .drop_duplicates(subset=['location_name', 'building'], 
                                                 keep='last'))
    
    def __get_equipmentid(self, 
                          df: "pandas dataFrame"):
        import pandas as pd
        f = lambda x: str(x).replace(' ', '_')
        device_name = [name.split('.')[0] for name in df.loc[:, 'device_name']]
        id = []
        for i in range(len(df)):
            a = device_name[i]
            b = df.loc[:, 'serial_number'][i]
            c = df.loc[:, 'rack_room_number'][i]
            id.append((lambda x, y, z:x + f(z) if pd.isna(y) else x + f(y))(a, b, c).replace('nan', ''))
        return id
    
    def update_equipment(self, 
                         date: "iso date str, format YYYY-MM-DD",
                         raw_df: "pandas dataFrame created from raw csv"):
        """update the dim_equipment dimension table."""
        import pandas as pd
        from sqlalchemy import create_engine
        engine = create_engine(self.engineStr)
        
        # get the columns
        df = (raw_df.loc[:, ['location', 'bldg', 'asset_tag', 'barcode', 
                           'device_name', 'device_type', 
                           'ip_address', 'make', 'model', 
                           'serial_number', 'simple_model', 
                           'port_count', 'primary_purpose', 
                           'category', 'purpose_id', 'rack_room_number', 
                           'replacement_cost']]
                    .rename(columns = {'location': 'location_name', 
                                       'bldg': 'building'})
              )
        ## add equipment_id
        df.insert(loc = 0,
                       column = 'equipment_id', 
                       value = self.__get_equipmentid(df))
        # insert location_key
        temp_df = df.merge(self.dim_location, 
                           how = "inner",
                           on=['location_name', 'building'], 
                           suffixes = ["", "_current"])
        temp_df = pd.concat([temp_df.loc[:, ['equipment_id', 'location_key']], 
                             temp_df.iloc[:, 3:18]], axis = 1)
                
        # compare with currently deployed dim_equipment
        temp_df = temp_df.merge(self.dim_equipment[self.dim_equipment['retirement_date'].apply(str) == '9999-12-31'], 
                                how = 'outer', 
                                on=['equipment_id'],
                                suffixes = ["", "_current"])
        
        # changed/retired devices
        self.retired_devices = temp_df[pd.isna(temp_df.device_name)]
        # set retirement_date/last_update_date to date
        with engine.begin() as connection:
            keys = [int(float(i)) for i in self.retired_devices['equipment_key'].tolist()]
            if len(keys) != 0:
                query = f"""
                UPDATE dim_equipment
                SET retirement_date = '{date}',
                    last_update_date = '{date}'
                WHERE equipment_key in ({keys});""".replace('[', '').replace(']', '')
                connection.execute(query)
        
        # new devices
        new_records = temp_df[pd.isna(temp_df.device_name_current)].iloc[:, 0:17]
        
        # set equipment_key, not needed if primary key is auto_increment
        # new_records.insert(loc = 0,
        #     column = 'equipment_key', 
        #     value = [i for i in range(len(self.dim_equipment) + 1, 
        #                                 len(self.dim_equipment) + len(new_records) + 1)])
        
        # set effective_date to date
        new_records['effective_date'] = date
        # set retirement_date to 9999-12-31
        new_records['retirement_date'] = '9999-12-31'
        # set last_update_date to date
        new_records['last_update_date'] = date
        # insert new records into database
        new_records.to_sql(name = 'dim_equipment', 
                con = engine, 
                if_exists = 'append', 
                index = False)

        self.dim_equipment = (pd.read_sql_query(self.query_dim_equipment, engine).
                                drop_duplicates(subset=['equipment_id'], keep='last'))
        
    def update_fact(self, 
                    date: "date the csv file is created",
                    df: "pandas dataFrame created from raw csv"):
        """Update fact table"""
        import pandas as pd
        from sqlalchemy import create_engine
        engine = create_engine(self.engineStr)
        
        # insert equipment_id then equipment_key 
        df.insert(loc = 0,
                column = 'equipment_id', 
                value = self.__get_equipmentid(df))
        df = (df.merge(self.dim_equipment, 
                      how = 'inner', 
                      on = ['equipment_id'],
                      suffixes = ["", "_dim_eq"])
                .rename(columns = {'location': 'location_name', 
                                       'bldg': 'building'})
            )
        # insert location_key
        df = df.merge(self.dim_location, 
                      how = 'inner', 
                      on = ['location_name', 'building'], 
                      suffixes = ['', '_dim_loc'])
        # insert date_key
        df['date_key'] = date
        # insert is_deployed as 1, and has_changed as 0
        df['is_deployed'] = 1
        df['has_changed'] = 0
        (df.loc[:, ['equipment_key', 'location_key',
                                     'date_key', 'has_changed',
                                     'is_deployed']]
            .to_sql(name = 'fact_inventory', 
                con = engine, 
                if_exists = 'append', 
                index = False)
        )
        # retired devices: insert has_changed as 1, is_deployed as 0, date_key as date
        self.retired_devices['date_key'] = date
        self.retired_devices['has_changed'] = 1
        self.retired_devices['is_deployed'] = 0        
        (self.retired_devices.loc[:, ['equipment_key', 'location_key',
                                     'date_key', 'has_changed',
                                     'is_deployed']]
            .to_sql(name = 'fact_inventory', 
                con = engine, 
                if_exists = 'append', 
                index = False)
        )

class CreateDash:
    """Creating Bokeh Plots"""
    def __init__(self,
                 engineStr: "database connection, for sqlalchemy engine"):
        self.engineStr = engineStr
    
    def stacked_bar(self, 
                    query: "query to get the data for stacked bar plot, col1=date, col2=deviceType, col3=changes",
                    title: "String of plot title",
                    num_date: "number of weeks needed on plot, default 52/1 year" = 52,
                    plot_width = 900,
                    plot_height = 1600):
        """Create the stacked bar chart for change by type & date plot"""
        
        from bokeh.io import output_file, show, save
        from bokeh.models import ColumnDataSource
        from bokeh.palettes import viridis, d3
        from bokeh.plotting import figure
        from bokeh.layouts import gridplot
        from sqlalchemy import create_engine
        import pandas as pd
        import os        
        engine = create_engine(self.engineStr)
                
        # get the df
        df_change_type = pd.read_sql_query(query, con = engine)
        df_change_type.date = df_change_type.date.astype('str')
        
        # get changes dict
        dates = sorted(list(set(df_change_type.date.tolist())), 
                       reverse = True)[0:num_date]
        device_type = list(set(df_change_type.deviceType.tolist()))
        changes = {}
        for dt in device_type:
            changes[dt] = []
        for dt in device_type:
            for d in dates:
                subset = df_change_type.query('date == @d')
                try:
                    changes[dt].append(subset.query('deviceType == @dt').changes.values[0])
                except IndexError:
                    changes[dt].append(0)
        changes['dates'] = dates
        
        # create stacked bar plot
        output_file(f"{os.getcwd().replace('/private', '')}/static/plots/change_by_type.html")
        source = ColumnDataSource(changes)
        stack = figure(y_range = dates, 
                       plot_height = plot_height, 
                       plot_width = plot_width,
                       title = title,
                       tools = 'ypan,box_zoom,wheel_zoom,reset,save,undo',
                       toolbar_location = 'right', 
                       tooltips="$name: @$name"
                       )
        stack.hbar_stack(device_type, y='dates', 
                        height=0.9, 
                        line_color="white",
                        color= (lambda x: x <= 20 and d3['Category20c'][x] or viridis(x))(len(device_type)),
                        source=source, 
                        legend_label=[f"{x}" for x in device_type]
                        )

        stack.y_range.range_padding = 0.1
        stack.ygrid.grid_line_color = None
        stack.legend.location = "top_right"
        stack.axis.minor_tick_line_color = None
        stack.outline_line_color = None
        stack.title.text_font_size = '25px'
        stack.outline_line_color = None

        new_legend = stack.legend[0]
        stack.add_layout(new_legend, 'right')
        grid = gridplot([[stack]], 
                    toolbar_location='right', 
                    merge_tools=True,
                    toolbar_options=dict(logo=None),
                    sizing_mode='stretch_both')
        save(grid)
        
        return grid
    
    def bar(self, 
            query: "query to get the data for bar plot",
            title: "String of plot title",
            x: "string, colomn name for x values",
            y: "string, column name for y values",
            bar_label: "string, column name for y values or bar labels",
            file_name: "string, output file name",
            plot_width = 800, 
            plot_height = 600):
        """Create change by date and deployed by date bar plot"""
        
        from bokeh.io import output_file, show, save
        from bokeh.models import ColumnDataSource, LabelSet
        from bokeh.plotting import figure
        from bokeh.layouts import gridplot
        from sqlalchemy import create_engine
        import pandas as pd
        import os
        
        engine = create_engine(self.engineStr)
        
        df_device = pd.read_sql_query(query, con = engine)
        df_device.date = df_device.date.astype('str')
        
        output_file(f"{os.getcwd().replace('/private', '')}/static/plots/{file_name}.html")
        
        deployed_by_date = figure(plot_width = plot_width, 
                                  plot_height = plot_height, 
                                  x_range = df_device.date.to_list(), 
                                  tools = "xpan,box_zoom,wheel_zoom,reset,undo,save", 
                                  title = title, 
                                  tooltips = [("# Device", f"@{y}"), 
                                              ("Date", "@date")])

        deployed_by_date.vbar(x = x, 
                              top = y, 
                              width = 0.5, 
                              source = df_device, 
                              color = (lambda x: '#fd8d3c' if y == 'changes' else '#6baed6')(y)
                              )

        deployed_by_date.y_range.start = 0
        deployed_by_date.yaxis.axis_label = "Number of Devices"

        deployed_by_date.x_range.range_padding = 0.05
        deployed_by_date.xgrid.grid_line_color = None
        deployed_by_date.xaxis.axis_label = "Date"
        deployed_by_date.xaxis.major_label_orientation = 1.57

        deployed_by_date.outline_line_color = None
        deployed_by_date.title.text_font_size = '25px'

        source = ColumnDataSource(df_device)

        label = LabelSet(x = x,
                        y = y, 
                        text = y,
                        level = 'glyph',
                        x_offset = (lambda x: -9 if y == 'changes' else -12)(y), 
                        y_offset = 0,
                        text_font_size = (lambda x: '9px' if y == 'changes' else '7px')(y),
                        source = source,
                        render_mode = 'canvas'
                        )
        deployed_by_date.add_layout(label)

        grid = gridplot([[deployed_by_date]], 
                    toolbar_location = 'right', 
                    merge_tools = True,
                    toolbar_options = dict(logo=None),
                    sizing_mode = 'stretch_both')
        save(grid)
        
        return grid
    
    def line(self, 
            query: "query to get the data for difference and confidence",
            title: "String of plot title",
            x: "string, colomn name for x values",
            y: "string, column name for y values",
            file_name: "string, output file name",
            plot_width = 800, 
            plot_height = 600):
        """Create difference and confidence line plot"""
        from bokeh.io import output_file, show, save
        from bokeh.models import ColumnDataSource, LabelSet, NumeralTickFormatter
        from bokeh.plotting import figure
        from bokeh.layouts import gridplot
        from bokeh.models.tools import HoverTool
        from sqlalchemy import create_engine
        import pandas as pd
        import os
        
        engine = create_engine(self.engineStr)
        
        df_con_diff = pd.read_sql_query(query, con = engine)
        df_con_diff['date'] = pd.to_datetime(df_con_diff['date'])
        # df_con_diff = df_con_diff.iloc[1:, :]
        df_con_diff["label"] = [str(round(i*100, 2)) + "%" for i in df_con_diff[y].tolist()]
        
        output_file(f"{os.getcwd().replace('/private', '')}/static/plots/{file_name}.html")
        
        conf_diff = figure(plot_width = plot_width, 
                      plot_height = plot_height, 
                      title = title, 
                      x_axis_type = "datetime",
                      tools="pan,box_zoom,wheel_zoom,undo,reset,save")
        
        source = ColumnDataSource(df_con_diff)
        
        conf_diff.circle(x=df_con_diff[x], 
                         y=df_con_diff[y], 
                         size=10, 
                         alpha=0.8,
                         color= (lambda x: '#756bb1' if y == 'diff' else '#31a354')(y)
                         )
        conf_diff.line(x, 
                       y,
                       source=source, 
                       line_width=2, 
                       line_alpha = 0.3,
                       color= (lambda x: '#756bb1' if y == 'diff' else '#31a354')(y)
                       )
        conf_diff.add_tools(HoverTool(tooltips=[("Date", "@date{%F}"), 
                                        (f"Inventory {file_name.capitalize()}", "@label")],
                                formatters={'@date': 'datetime'}
                                ))

        conf_diff.yaxis.axis_label = file_name.capitalize()
        conf_diff.xaxis.axis_label = "Date"

        conf_diff.yaxis.formatter = NumeralTickFormatter(format='%0.0f %%')

        conf_diff.outline_line_color = None
        conf_diff.title.text_font_size = '25px'

        label = LabelSet(x = x, 
                         y = y, 
                         text = 'label',
                         x_offset = -10, 
                         y_offset = 5,
                         text_font_size = '10px',
                         source = source,
                         render_mode = 'canvas'
                         )
        conf_diff.add_layout(label)

        grid = gridplot([[conf_diff]], 
                        toolbar_location='right', 
                        merge_tools=True,
                        toolbar_options=dict(logo=None),
                        sizing_mode='stretch_both')
        save(grid)
        
        return grid
    
    def update_summary(self):
        """Prepare data for the summary table."""
        import os
        import pandas as pd
        from sqlalchemy import create_engine
        
        engine = create_engine(self.engineStr)
        dir = os.getcwd().replace('private', '')

        # Average number of deployed of Device by date
        query = '''
        SELECT `date`, ROUND(AVG(num_device), 2) as avg_num_device
        FROM (
                SELECT date_key as `date`, SUM(is_deployed) as num_device
                FROM fact_inventory fi 
                group by date_key
        ) as temp
        ;
        '''
        df_num_device = pd.read_sql_query(query, con = engine)
        
        # Average number of Changes by date
        query = '''
        SELECT `date`, ROUND(AVG(num_changes), 2) as avg_num_changes
        FROM (
                SELECT date_key as `date`, SUM(has_changed) as num_changes
                FROM fact_inventory fi 
                group by date_key
        ) as temp
        ;
        '''
        df_num_changes = pd.read_sql_query(query, con = engine)

        # Average Operational Invertory Confidence and Average Inventory Difference
        inventory_diff = round(df_num_changes.iloc[0, 1] / df_num_device.iloc[0, 1] * 100, 2)
        inventory_conf = 100 - inventory_diff

        ## Total number of Devices Deployed, formula from excel spreadsheet
        query = '''
        SELECT date_key as `Date`, SUM(is_deployed) as num_deployed
        FROM fact_inventory fi 
        group by date_key 
        ;
        '''
        df_deployed = pd.read_sql_query(query, con = engine)
        total_num_device = df_deployed.iloc[df_deployed.shape[0] - 1, 1] - df_deployed.iloc[0, 1] + 200

        json_out = f'''var summary = [{{
            'avg_dev': {df_num_device.iloc[0, 1]}, 
            'avg_change': {df_num_changes.iloc[0, 1]}, 
            'conf': "{inventory_conf}%", 
            'diff': "{inventory_diff}%", 
            'total_dev': {total_num_device}
        }}]
        '''

        with open(f'{dir}/static/text/summary.js', 'w') as summary:
            summary.write(json_out)
    
    def export_csv(self, 
                   start_date: "selected start date",
                   end_date: "selected end date",
                   click: "nth time the submit button is clicked",
                   rand: "a 5-digit random number"):
        """Creat csv files for downloading"""
        import pandas as pd
        from sqlalchemy import create_engine
        import os
        
        engine = create_engine(self.engineStr)
        dir = os.getcwd().replace('private', '/static/csv_reports/')
        
        ## change by type by date
        query = f'''
        SELECT fi.date_key as `date`, de.device_type as deviceType, SUM(fi.has_changed) as changes
        FROM fact_inventory fi 
        join dim_equipment de
        on fi.equipment_key = de.equipment_key
        WHERE fi.has_changed = 1
        AND fi.date_key between '{start_date}' and '{end_date}'
        GROUP BY fi.date_key, de.device_type
        ORDER BY `date`, changes desc
        ;
        '''
        df_change_type = pd.read_sql_query(query, con = engine)
        df_change_type.to_csv(
            f'{dir}change_by_type_by_date_{click}_{rand}.csv', 
            index = False    
        )

        ## change by date
        query = f'''
        SELECT date_key as `date`, SUM(has_changed) as changes
        FROM fact_inventory fi 
        WHERE fi.date_key between '{start_date}' and '{end_date}'
        group by `date` 
        ;
        '''
        df_change = pd.read_sql_query(query, con = engine)
        df_change.to_csv(
            f'{dir}change_by_date_{click}_{rand}.csv', 
            index = False   
        )

        ## number of deployed by date
        query = f'''
        SELECT date_key as `date`, SUM(is_deployed) as deployed
        FROM fact_inventory fi 
        WHERE fi.date_key between '{start_date}' and '{end_date}'
        group by `date` 
        ;
        '''
        df_device = pd.read_sql_query(query, con = engine)
        df_device.to_csv(
            f'{dir}deployed_{click}_{rand}.csv', 
            index = False   
        )

        ## confidence & difference
        query = f'''
        SELECT date_key as `date`, SUM(has_changed) * 1.0 / SUM(is_deployed) * 1.0 as difference, 
        1 - SUM(has_changed) * 1.0 / SUM(is_deployed) * 1.0 as confidence
        FROM fact_inventory fi 
        WHERE fi.date_key between '{start_date}' and '{end_date}'
        group by date_key 
        ;
        '''
        df_con_diff = pd.read_sql_query(query, con = engine)
        df_con_diff.to_csv(
            f'{dir}confidence_difference_{click}_{rand}.csv', 
            index = False  
        )