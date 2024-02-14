import numpy as np
from fastapi import FastAPI, Request
import requests
from fastapi.encoders import jsonable_encoder
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import uvicorn
import time
import math
import pandas as pd
import datetime
import statistics
import json
from app.wfl0_function import get_realtime_millisecond_timestamp,convert_to_json_serializable, get_one_hour_before_realtime_millisecond_timestamp, assignOperatorToTag, \
    create_shift_dict, AssignShiftId,fetchProximityInTime ,fetchPlannedProductionTime,calculateEfficiencyTime,fetchActualProductionTime ,fetchTargetPart,fetchTotalPartsProduced,calculateEfficiencyToProducedParts ,\
fetchQualityPercentage,getOle,assign_org_unit_dept ,getBefore_n_DaysEpochTimestamp,get_start_epoch_time_of_yesterdays_month,getBefore_n_MonthsEpochTimestamp

config_data = {
    "graphql_url": "https://api.humac.live/hasura/v1/graphql",
    "headers": {
        "x-hasura-admin-secret": "VI5BaLwzSQV3BPIx",
        "Content-Type": "application/json"
    }
}
transport = AIOHTTPTransport(
    url=config_data['graphql_url'], headers=config_data['headers'])
client = Client(transport=transport, fetch_schema_from_transport=True)

async def get_data(query, variables):
    try:
        result = await client.execute_async(query, variables)
        #print(result)
        return result
    except Exception as e:
        print(e)
async def get_data1(query):
    try:
        result = await client.execute_async(query)
        # print(result)
        return result
    except Exception as e:
        print(e)


async def send_mutation(mutation, params):
    try:
        #print(params)
        result = await client.execute_async(mutation, variable_values=params)
        print(result)
    except Exception as e:
        print(e)

app = FastAPI()
@app.get("/")
async def root():
    print("Hello")
    return{"message":"hello : HUMAC"}

def get_midnight_epoch_timestamp():
    current_datetime = datetime.datetime.now()
    midnight_datetime = datetime.datetime.combine(current_datetime.date(), datetime.time.min)
    midnight_timestamp = int(midnight_datetime.timestamp() * 1000)
    return midnight_timestamp

midnight_ts = get_midnight_epoch_timestamp()
#print(midnight_ts)
realtime_millisecond_timestamp = get_realtime_millisecond_timestamp()
#print(realtime_millisecond_timestamp)
one_hour_before_timestamp = get_one_hour_before_realtime_millisecond_timestamp()
#print(one_hour_before_timestamp)


async def getProximityData(variables):
    query = gql(
        """
        query MyQuery($ts: bigint = "ts") {
          proximity(where: {ts: {_gte: $ts}}) {
            ts
            tenantid
            machineid
            edgeid
            tagid
            state
          }
        }

        """)
    data = await get_data(query, variables)
    return data['proximity']


async def getShiftDict():
    query2 = gql("""
    query get_shift_data {
            tnt_work_schedule_masters {
                break1_name
                break2_name
                break_one_end_time
                break_one_start_time
                break_two_end_time
                break_two_start_time
                organization_id
                plant
                shift_end_time
                shift_name
                shift_start_time
                shift_type
                tenantid
            }
        }
    """)
    data = await get_data1(query2)
    return data['tnt_work_schedule_masters']

async def getDmShift(variable):
    query = gql("""
        query MyQuery($ts: bigint = "") {
          dm_shiftwise(where: {timestamp: {_gte: $ts}}) {
            date
            machineid
            parts_per_minute
            planned_production_time
            quality_percent
            shift_id
            target_parts
            tenantid
            total_machine_runtime
            total_parts_produced
            actual_production_time
            no_of_parts_rejected
            timestamp
          }
        }

    """)
    data = await get_data(query,variable)
    return data['dm_shiftwise']


async def getWfl0OperatorMachineAvailabilityDf(variable):
    query = gql("""
        query MyQuery($ts: bigint = "") {
          wfl0_operator_machine_availability(where: {timestamp: {_gte: $ts}}) {
            date
            department_id
            edgeid
            machine_proximity_time_loss_percent
            machineid
            operator_id
            org_id
            proximity_in_time_total
            proximity_out_time_total
            shift
            tagid
            tenantid
            timestamp
            total_effective_proximity_time
            unit_id
          }
        }
        
            """)
    data = await get_data(query, variable)
    return data['wfl0_operator_machine_availability']

async def getEmpTagAssignData():
    query1 = gql(
        """
        query MyQuery {
          employee_tag_assignment {
            tenantid
            tag_id
            employee_id
          }
        }

        """)
    data = await get_data1(query1)
    return data['employee_tag_assignment']

async def getWfl0OverallLaborEffectiveness(variable):
    query2 = gql("""
    query MyQuery( $timestamp: bigint = "") {
          wfl0_overall_labor_effectiveness(where: {timestamp: {_gte: $timestamp}}) {
            actual_production_time
            date
            department_id
            edgeid
            machineid
            operator_id
            org_id
            overall_labor_effectiveness
            planned_production_time
            production_efficiency_per
            proximity_in_time
            quality_efficiency_per
            shift
            target_parts
            tenantid
            time_efficiency_per
            timestamp
            total_parts_produced
            unit_id
          }
        }

    """)
    data = await get_data(query2,variable)
    return data['wfl0_overall_labor_effectiveness']

# to get tnt_org_machine_assignment df
@app.post("/overall_labor_effectiveness")
async def workforceEfficiency():
    try:
        query2 = gql(
            """
            query MyQuery {
                tnt_org_machine_assignment {
                    org_id
                    unit
                    department
                    tenantid
                    machineid
                    edgeid
                    machine_auto_id
                }
            }
            """
        )
        s2 = await get_data1(query2)
        # variables = {"timestamp": midnight_ts}
        # print(variables)
        # tnt_org_machine_assignment to assign org_id,department_id, unit_id
        variables = {"ts": 1704690000000}
        df = pd.DataFrame(await getProximityData(variables))
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms")
        shift_id_df = pd.DataFrame(await getShiftDict())
        shift_id_dict = create_shift_dict(shift_id_df)
        df["shift_id"] = AssignShiftId(df, shift_id_dict)
        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)
        if df.empty:
            return {"status": "Required Data is not available"}
        # print(df)
        # dm_shftwise table
        df1 = pd.DataFrame(await getDmShift(variables))
        print(df1)
        # if df1.empty:
        #     return {"status": "Required Data is not available"}

        # dm_operator_machine_availability
        df2 = pd.DataFrame(await getWfl0OperatorMachineAvailabilityDf(variables))
        print(df2)

        if df2.empty:
            return {"status": "Required Data is not available"}

        operator_tag_dict = await getEmpTagAssignData()

        separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in df.groupby(df['timestamp'].dt.date)}
        workforce_efficiency_dict = {}
        for date, df_date in separate_dfs.items():
            workforce_efficiency_dict[date] = {}

            # Iterate over hours for the current machine
            for (machine_id, shift_id, tenant_id, tag_id), df_group in df_date.groupby([df_date['machineid'], df_date['shift_id'], df_date['tenantid'], df_date['tagid']]):
                operator_id = assignOperatorToTag(df['tenantid'].iloc[0], tag_id, operator_tag_dict)
                proximity_in_time = fetchProximityInTime(df2, date, tenant_id, df_group['machineid'].iloc[-1], operator_id, shift_id)
                planned_production_time = fetchPlannedProductionTime(df1, date, tenant_id,df_group['machineid'].iloc[-1], shift_id)
                actual_production_time = fetchActualProductionTime(df1, date, tenant_id, df_group['machineid'].iloc[-1],shift_id)
                time_efficiency = calculateEfficiencyTime(proximity_in_time, planned_production_time)
                total_parts_produced = fetchTotalPartsProduced(df1, date, tenant_id, machine_id, shift_id)
                target_parts = fetchTargetPart(df1, date, tenant_id, machine_id, shift_id)
                production_efficiency = calculateEfficiencyToProducedParts(total_parts_produced, target_parts)
                quality_efficiency_per = fetchQualityPercentage(df1, date, tenant_id, machine_id, shift_id)
                ole = getOle(time_efficiency, production_efficiency, quality_efficiency_per)

                # Store data in workforce_efficiency_dict under appropriate keys
                workforce_efficiency_dict[date][shift_id]= {
                    'date': date,
                    'timestamp': df_group['ts'].iloc[-1],
                    'tenantid': tenant_id,
                    'machineid': machine_id,
                    'edgeid': df_group['edgeid'].iloc[-1],
                    'org_id': df_group['org_id'].iloc[-1],
                    'unit_id': df_group['unit_id'].iloc[-1],
                    'department_id': df_group['unit_id'].iloc[-1],
                    'operator_id': operator_id,
                    'proximity_in_time': round(proximity_in_time) if proximity_in_time is not None else None,
                    'time_efficiency_per': time_efficiency,
                    'quality_efficiency_per': quality_efficiency_per,
                    'planned_production_time': planned_production_time,
                    'actual_production_time': actual_production_time,
                    'total_parts_produced': total_parts_produced,
                    'target_parts': target_parts,
                    'production_efficiency_per': production_efficiency,
                    'shift': shift_id,
                    'overall_labor_effectiveness': ole
                }

        individual_workforce_efficiency_dict = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_overall_labor_effectiveness_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_overall_labor_effectiveness(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_overall_labor_effectiveness_table, params_delete)

        for date, operator_machine_availability_dict in workforce_efficiency_dict.items():
            variable_name = f"workforce_efficiency_{date}"
            individual_workforce_efficiency_dict[variable_name] = operator_machine_availability_dict

            # Integrate the two lines here
            formatted_data = [item[1] for item in operator_machine_availability_dict.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
           # print(formatted_data)
            mutation_objects = formatted_data
            wfl0_overall_labor_effectiveness_table = gql(
                """
                mutation MyMutation($objects: [wfl0_overall_labor_effectiveness_insert_input!] = {}) {
                          insert_wfl0_overall_labor_effectiveness(objects: $objects) {
                            affected_rows
                          }
                        }
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_overall_labor_effectiveness_table, params)

        return {
            "status": "SUCCESS",

        }
    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }

@app.post("/shiftwise_workforce_efficiency")
async def shiftwiseWorkforceefficiency():
    try:
        # variables = {"timestamp": midnight_ts}
        # print(variables)
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0OverallLaborEffectiveness(variable))
        if df.empty:
            return {"status": "Required Data is not available"}
        #print(df)
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
       # print(df['operator_id'])
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        workforce_efficiency_shiftly_time = {}

        for department_id, df_dept in separate_dfs.items():
            #print(department_id)
            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_efficiency_shiftly_time[department_id] = {}

            for (day, shift), day_group in df_dept.groupby([df_dept['timestamp1'].dt.to_period('D'), 'shift']):

                workforce_dict = {
                    'date': day_group['date'].iloc[-1],
                    'timestamp': day_group['timestamp'].iloc[-1],
                    'tenantid':day_group['tenantid'].iloc[-1] ,
                    'machineid': day_group['machineid'].iloc[-1],
                    'edgeid': day_group['edgeid'].iloc[-1],
                    'org_id':day_group['org_id'].iloc[-1],
                    'unit_id': day_group['unit_id'].iloc[-1],
                    'department_id': day_group['unit_id'].iloc[-1],
                    'shift': day_group['shift'].iloc[-1],
                    'operator_id': day_group['operator_id'].iloc[-1] if not pd.isnull(day_group['operator_id'].iloc[-1]) else None,
                    'proximity_in_time': day_group['proximity_in_time'].sum() ,
                    'time_efficiency_per': (day_group['time_efficiency_per'].mean(skipna=True)) if not np.isnan(day_group['time_efficiency_per'].mean(skipna=True)) else None,
                    'quality_efficiency_per': (day_group['quality_efficiency_per'].mean(skipna=True)) if not np.isnan(day_group['quality_efficiency_per'].mean(skipna=True)) else None,
                    'planned_production_time': day_group['planned_production_time'].sum() ,
                    'actual_production_time':day_group['actual_production_time'].sum() ,
                    'total_parts_produced': day_group['total_parts_produced'].sum(),
                    'target_parts': day_group['target_parts'].sum(),
                    'production_efficiency_per': (day_group['production_efficiency_per'].mean(skipna=True)) if not np.isnan(day_group['production_efficiency_per'].mean(skipna=True)) else None,
                    'overall_labor_effectiveness': (day_group['overall_labor_effectiveness'].mean(skipna=True)) if not np.isnan(day_group['overall_labor_effectiveness'].mean(skipna=True)) else None
                }

                workforce_efficiency_shiftly_time[department_id][shift] = workforce_dict
           # print(workforce_efficiency_daily_time)
        individual_workforce_efficiency_shiftly_time_dict = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_workforce_efficiency_shiftwise_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_efficiency_shiftwise(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_efficiency_shiftwise_table, params_delete)

        for department_id, workforce_efficiency_shiftly_time_dict_row in workforce_efficiency_shiftly_time.items():
            variable_name = f"workforce_efficiency_{department_id}"
            #print(variable_name)
            individual_workforce_efficiency_shiftly_time_dict[variable_name] = workforce_efficiency_shiftly_time_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_efficiency_shiftly_time_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
           # print(mutation_objects)
            wflo_workforce_efficiency_shiftwise_table = gql(
                """
               mutation MyMutation($objects: [wfl0_workforce_efficiency_shiftwise_insert_input!] = {}) {
                  insert_wfl0_workforce_efficiency_shiftwise(objects: $objects) {
                    affected_rows
                  }
                }
    
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_efficiency_shiftwise_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }



@app.post("/Daily_workforce_efficiency")
async def dailyWorkforceefficiency():
    try:
       # variables = {"timestamp": midnight_ts}
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0OverallLaborEffectiveness(variable))
        if df.empty:
            return {"status": "Required Data is not available"}
        #print(df)
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
       # print(df['operator_id'])
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        workforce_efficiency_daily_time = {}

        for department_id, df_dept in separate_dfs.items():
            #print(department_id)
            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_efficiency_daily_time[department_id] = {}

            for day, day_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('D')):

                workforce_dict = {
                    'date': day_group['date'].iloc[-1],
                    'timestamp': day_group['timestamp'].iloc[-1],
                    'tenantid':day_group['tenantid'].iloc[-1] ,
                    'machineid': day_group['machineid'].iloc[-1],
                    'edgeid': day_group['edgeid'].iloc[-1],
                    'org_id':day_group['org_id'].iloc[-1],
                    'unit_id': day_group['unit_id'].iloc[-1],
                    'department_id': day_group['unit_id'].iloc[-1],
                    'shift': day_group['shift'].iloc[-1],
                    'operator_id': day_group['operator_id'].iloc[-1] if not pd.isnull(day_group['operator_id'].iloc[-1]) else None,
                    'proximity_in_time': day_group['proximity_in_time'].sum() ,
                    'time_efficiency_per': (day_group['time_efficiency_per'].mean(skipna=True)) if not np.isnan(day_group['time_efficiency_per'].mean(skipna=True)) else None,
                    'quality_efficiency_per': (day_group['quality_efficiency_per'].mean(skipna=True)) if not np.isnan(day_group['quality_efficiency_per'].mean(skipna=True)) else None,
                    'planned_production_time': day_group['planned_production_time'].sum() ,
                    'actual_production_time':day_group['actual_production_time'].sum() ,
                    'total_parts_produced': day_group['total_parts_produced'].sum(),
                    'target_parts': day_group['target_parts'].sum(),
                    'production_efficiency_per': (day_group['production_efficiency_per'].mean(skipna=True)) if not np.isnan(day_group['production_efficiency_per'].mean(skipna=True)) else None,
                    'overall_labor_effectiveness': (day_group['overall_labor_effectiveness'].mean(skipna=True)) if not np.isnan(day_group['overall_labor_effectiveness'].mean(skipna=True)) else None
                }

                workforce_efficiency_daily_time[department_id][day] = workforce_dict
           # print(workforce_efficiency_daily_time)
        individual_workforce_efficiency_daily_time = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_workforce_efficiency_daily_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_efficiency_daily(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_efficiency_daily_table, params_delete)

        for department_id, workforce_efficiency_daily_dict_row in workforce_efficiency_daily_time.items():
            variable_name = f"workforce_efficiency_{department_id}"
            #print(variable_name)
            individual_workforce_efficiency_daily_time[variable_name] = workforce_efficiency_daily_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_efficiency_daily_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
           # print(mutation_objects)
            wflo_workforce_efficiency_daily_table = gql(
                """
               mutation MyMutation($objects: [wfl0_workforce_efficiency_daily_insert_input!] = {}) {
                  insert_wfl0_workforce_efficiency_daily(objects: $objects) {
                    affected_rows
                  }
                }
    
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_efficiency_daily_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }

@app.post("/weekly_workforce_efficiency")
async def weeklyWorkforceefficiency():
    try:
        #variables = {"timestamp": getBefore_n_DaysEpochTimestamp(7)}
       # print(getBefore_n_DaysEpochTimestamp(7))
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0OverallLaborEffectiveness(variable))
        if df.empty:
            return {"status": "Required Data is not available"}
        #print(df)
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")

        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        workforce_efficiency_weekly_time = {}

        for department_id, df_dept in separate_dfs.items():
            #print(department_id)
            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_efficiency_weekly_time[department_id] = {}

            for week, week_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('W')):
                workforce_dict = {
                    'start_date': week.start_time.strftime("%Y-%m-%d"),
                    'end_date': week.end_time.strftime("%Y-%m-%d"),
                    'timestamp': week_group['timestamp'].iloc[-1],
                    'tenantid':week_group['tenantid'].iloc[-1] ,
                    'machineid': week_group['machineid'].iloc[-1],
                    'edgeid': week_group['edgeid'].iloc[-1],
                    'org_id':week_group['org_id'].iloc[-1],
                    'unit_id': week_group['unit_id'].iloc[-1],
                    'department_id': week_group['unit_id'].iloc[-1],
                    'shift': week_group['shift'].iloc[-1],
                    'operator_id':week_group['operator_id'].iloc[-1],
                    'proximity_in_time': week_group['proximity_in_time'].sum() ,
                    'time_efficiency_per': (week_group['time_efficiency_per'].mean(skipna=True)) if not np.isnan(week_group['time_efficiency_per'].mean(skipna=True)) else None,
                    'quality_efficiency_per': (week_group['quality_efficiency_per'].mean(skipna=True)) if not np.isnan(week_group['quality_efficiency_per'].mean(skipna=True)) else None,
                    'planned_production_time': week_group['planned_production_time'].sum() ,
                    'actual_production_time':week_group['actual_production_time'].sum() ,
                    'total_parts_produced': week_group['total_parts_produced'].sum(),
                    'target_parts': week_group['target_parts'].sum(),
                    'production_efficiency_per': (week_group['production_efficiency_per'].mean(skipna=True)) if not np.isnan(week_group['production_efficiency_per'].mean(skipna=True)) else None,
                    'overall_labor_effectiveness': (week_group['overall_labor_effectiveness'].mean(skipna=True)) if not np.isnan(week_group['overall_labor_effectiveness'].mean(skipna=True)) else None
                }

                workforce_efficiency_weekly_time[department_id][week] = workforce_dict
           # print(workforce_efficiency_daily_time)
        individual_workforce_efficiency_weekly_time = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_workforce_efficiency_weekly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_efficiency_weekly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_efficiency_weekly_table, params_delete)

        for department_id, workforce_efficiency_week_dict_row in workforce_efficiency_weekly_time.items():
            variable_name = f"workforce_efficiency_{department_id}"
            #print(variable_name)
            individual_workforce_efficiency_weekly_time[variable_name] = workforce_efficiency_week_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_efficiency_week_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            #print(mutation_objects)
            wflo_workforce_efficiency_weekly_table = gql(
                """
               mutation MyMutation($objects: [wfl0_workforce_efficiency_weekly_insert_input!] = {}) {
                  insert_wfl0_workforce_efficiency_weekly(objects: $objects) {
                    affected_rows
                  }
                }
    
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_efficiency_weekly_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }

@app.post("/monthly_workforce_efficiency")
async def monthlyWorkforceefficiency():
    try:
        variable = {"timestamp": 1704689530000}
        timestamp = get_start_epoch_time_of_yesterdays_month()
        print(timestamp)
        df = pd.DataFrame(await getWfl0OverallLaborEffectiveness(variable))
        if df.empty:
            return {"status": "Required Data is not available"}
        #print(df)
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")

        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        workforce_efficiency_monthly_time = {}

        for department_id, df_dept in separate_dfs.items():
            #print(department_id)
            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_efficiency_monthly_time[department_id] = {}

            for month, month_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('M')):
                workforce_dict = {
                    'start_date': month.start_time.strftime("%Y-%m-%d"),
                    'end_date': month.end_time.strftime("%Y-%m-%d"),
                    'timestamp': month_group['timestamp'].iloc[-1],
                    'tenantid':month_group['tenantid'].iloc[-1] ,
                    'machineid': month_group['machineid'].iloc[-1],
                    'edgeid': month_group['edgeid'].iloc[-1],
                    'org_id':month_group['org_id'].iloc[-1],
                    'unit_id': month_group['unit_id'].iloc[-1],
                    'department_id': month_group['unit_id'].iloc[-1],
                    'shift': month_group['shift'].iloc[-1],
                    'operator_id':month_group['operator_id'].iloc[-1] if not pd.isnull(month_group['operator_id'].iloc[-1]) else None,
                    'proximity_in_time': month_group['proximity_in_time'].sum() ,
                    'time_efficiency_per': (month_group['time_efficiency_per'].mean(skipna=True)) if not np.isnan(month_group['time_efficiency_per'].mean(skipna=True)) else None,
                    'quality_efficiency_per': (month_group['quality_efficiency_per'].mean(skipna=True)) if not np.isnan(month_group['quality_efficiency_per'].mean(skipna=True)) else None,
                    'planned_production_time': month_group['planned_production_time'].sum() ,
                    'actual_production_time':month_group['actual_production_time'].sum() ,
                    'total_parts_produced': month_group['total_parts_produced'].sum(),
                    'target_parts': month_group['target_parts'].sum(),
                    'production_efficiency_per': (month_group['production_efficiency_per'].mean(skipna=True)) if not np.isnan(month_group['production_efficiency_per'].mean(skipna=True)) else None,
                    'overall_labor_effectiveness': (month_group['overall_labor_effectiveness'].mean(skipna=True)) if not np.isnan(month_group['overall_labor_effectiveness'].mean(skipna=True)) else None
                }

                workforce_efficiency_monthly_time[department_id][month] = workforce_dict
           # print(workforce_efficiency_daily_time)
        individual_workforce_efficiency_daily_time = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_workforce_efficiency_monthly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_efficiency_monthly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_efficiency_monthly_table, params_delete)

        for department_id, workforce_efficiency_monthlydict_row in workforce_efficiency_monthly_time.items():
            variable_name = f"workforce_efficiency_{department_id}"
            print(variable_name)
            individual_workforce_efficiency_daily_time[variable_name] = workforce_efficiency_monthlydict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_efficiency_monthlydict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            #print(mutation_objects)
            wflo_workforce_efficiency_monthly_table = gql(
                """
              mutation MyMutation($objects: [wfl0_workforce_efficiency_monthly_insert_input!] = {}) {
                  insert_wfl0_workforce_efficiency_monthly(objects: $objects) {
                    affected_rows
                  }
                }
    
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_efficiency_monthly_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/yearly_workforce_efficiency")
async def yearlyWorkforceefficiency():
    try:
        variables = {"timestamp": getBefore_n_MonthsEpochTimestamp(12)}
        print(getBefore_n_MonthsEpochTimestamp(12))
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0OverallLaborEffectiveness(variable))
        if df.empty:
            return {"status": "Required Data is not available"}
        #print(df)
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")

        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        workforce_efficiency_yearly_time = {}

        for department_id, df_dept in separate_dfs.items():
            #print(department_id)
            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_efficiency_yearly_time[department_id] = {}

            for year, year_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('Y')):
                workforce_dict = {
                    'start_date': year.start_time.strftime("%Y-%m-%d"),
                    'end_date': year.end_time.strftime("%Y-%m-%d"),
                    'timestamp': year_group['timestamp'].iloc[-1],
                    'tenantid':year_group['tenantid'].iloc[-1] ,
                    'machineid': year_group['machineid'].iloc[-1],
                    'edgeid': year_group['edgeid'].iloc[-1],
                    'org_id':year_group['org_id'].iloc[-1],
                    'unit_id': year_group['unit_id'].iloc[-1],
                    'department_id': year_group['unit_id'].iloc[-1],
                    'shift': year_group['shift'].iloc[-1],
                    'operator_id':year_group['operator_id'].iloc[-1]  if not pd.isnull(year_group['operator_id'].iloc[-1]) else None,
                    'proximity_in_time': year_group['proximity_in_time'].sum() ,
                    'time_efficiency_per': (year_group['time_efficiency_per'].mean(skipna=True)) if not np.isnan(year_group['time_efficiency_per'].mean(skipna=True)) else None,
                    'quality_efficiency_per': (year_group['quality_efficiency_per'].mean(skipna=True)) if not np.isnan(year_group['quality_efficiency_per'].mean(skipna=True)) else None,
                    'planned_production_time': year_group['planned_production_time'].sum() ,
                    'actual_production_time':year_group['actual_production_time'].sum() ,
                    'total_parts_produced': year_group['total_parts_produced'].sum(),
                    'target_parts': year_group['target_parts'].sum(),
                    'production_efficiency_per': (year_group['production_efficiency_per'].mean(skipna=True)) if not np.isnan(year_group['production_efficiency_per'].mean(skipna=True)) else None,
                    'overall_labor_effectiveness': (year_group['overall_labor_effectiveness'].mean(skipna=True)) if not np.isnan(year_group['overall_labor_effectiveness'].mean(skipna=True)) else None
                }

                workforce_efficiency_yearly_time[department_id][year] = workforce_dict
           # print(workforce_efficiency_daily_time)
        individual_workforce_efficiency_yearly_time = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_workforce_efficiency_yearly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_efficiency_yearly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_efficiency_yearly_table, params_delete)

        for department_id, workforce_efficiency_yearly_dict_row in workforce_efficiency_yearly_time.items():
            variable_name = f"workforce_efficiency_{department_id}"
            print(variable_name)
            individual_workforce_efficiency_yearly_time[variable_name] = workforce_efficiency_yearly_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_efficiency_yearly_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            #print(mutation_objects)
            wflo_workforce_efficiency_yearly_table = gql(
                """
               mutation MyMutation($objects: [wfl0_workforce_efficiency_yearly_insert_input!] = {}) {
                  insert_wfl0_workforce_efficiency_yearly(objects: $objects) {
                    affected_rows
                  }
                }
    
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_efficiency_yearly_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }