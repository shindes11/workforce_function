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
    create_shift_dict, AssignShiftId ,fetchProximityInTime ,assign_org_unit_dept ,fetchRunTime,calculateWorkforceUtilization ,fetchJobRole,\
fetchSkill,fetchExperience,fetchEducation,fetchSalary,fetchCertification,fetchGender,getBefore_n_DaysEpochTimestamp,get_start_epoch_time_of_yesterdays_month

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
    data = await get_data(query, variable)
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

async def getWfl0WorkforceUtilization(variable):
    query2 = gql("""
    query MyQuery($timestamp: bigint = "") {
          wfl0_workforce_utilization(where: {timestamp: {_gte: $timestamp}}) {
            date
            department_id
            edgeid
            machineid
            operator_id
            org_id
            proximity_in_time
            shift
            tenantid
            timestamp
            total_run_time
            unit_id
            workforce_utilization_per
            skill
            salary
            job_role
            gender
            experience
            education
            certification
          }
        }

        """)
    data = await get_data(query2, variable)
    return data['wfl0_workforce_utilization']
async def getwfl0OeratorProximityLogDf(variable):
    query2 = gql("""
    query MyQuery($ts: bigint = "") {
          wfl0_operator_proximity_log(where: {timestamp: {_gte: $ts}}) {
            certification
            date
            edgeid
            education
            end_customer
            experience
            gender
            job_role
            machineid
            marital_status
            operator_id
            part_id
            salary
            skill
            tagid
            tenantid
            timestamp

          }
        }

        """)
    data = await get_data(query2, variable)
    return data['wfl0_operator_proximity_log']





@app.post("/workfoce_utilization")
async def workforceutilization():
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
    s2 = await get_data1(query2)  # tnt_org_machine_assignment to assign org_id,department_id, unit_id
    # variables = {"timestamp": midnight_ts}
    # print(variables)
    variables = {"ts": 1704699593000}
    df = pd.DataFrame(await getProximityData(variables))
    df['timestamp'] = pd.to_datetime(df['ts'], unit="ms", origin='unix')
    shift_id_df = pd.DataFrame(await getShiftDict())
    shift_id_dict = create_shift_dict(shift_id_df)
    df["shift_id"] = AssignShiftId(df, shift_id_dict)
    assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
    df = assign_org_unit_dept(df, assignment_dict)
    if df.empty:
        return {"status": "Required Data is not available"}
    df3 = pd.DataFrame(await getwfl0OeratorProximityLogDf(variables))
    #print(df3)

    # dm_shftwise table
    df1 = pd.DataFrame(await getDmShift(variables))
    if df1.empty:
        return {"status": "Required Data is not available"}

    # wfl0_operator_machine_availability table
    df2 = pd.DataFrame(await getWfl0OperatorMachineAvailabilityDf(variables))
    if df.empty:
        return {"status": "Required Data is not available"}

    operator_tag_dict = await getEmpTagAssignData()

    separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in df.groupby(df['timestamp'].dt.date)}
    workforce_utilization_dict = {}
    for date, df_date in separate_dfs.items():
        workforce_utilization_dict[date] = {}
        for (tenant_id, shift_id, tag_id, machine_id), df_group in df_date.groupby([df_date['tenantid'], df_date['shift_id'], df_date['tagid'], df_date['machineid']]):
            operator_id = assignOperatorToTag(df_group['tenantid'].iloc[-1], tag_id, operator_tag_dict)
            job_role = fetchJobRole(df3, operator_id)
            skill = fetchSkill(df3, operator_id)
            experience = fetchExperience(df3, operator_id)
            salary = fetchSalary(df3, operator_id)
            gender = fetchGender(df3, operator_id)
            education = fetchEducation(df3, operator_id)
            certification = fetchCertification(df3, operator_id)
            proximity_in_time = fetchProximityInTime(df2, date, tenant_id, df_group['machineid'].iloc[-1], operator_id, shift_id)
            total_run_time = fetchRunTime(df1, date, tenant_id, machine_id, shift_id)
            workforce_utilization_per = calculateWorkforceUtilization(proximity_in_time, total_run_time)
            #print(operator_id,job_role)
            workforce_utilization_dict_individual = {
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
                'total_run_time': total_run_time,
                'workforce_utilization_per': workforce_utilization_per,
                'shift': shift_id,
                'job_role': job_role,
                'skill': skill,
                'experience': experience,
                'salary': salary,
                'gender': gender,
                'education': education,
                'certification': certification

            }
           # print(workforce_utilization_dict_individual)

            workforce_utilization_dict[date][shift_id] = workforce_utilization_dict_individual
    # Push individual machine actual production times to the database
    individual_workforce_utilization_dict = {}
    params_delete = {"timestamp": midnight_ts}
    wfl0_workforce_utilization_table = gql(
        """
         mutation MyMutation($timestamp: bigint = "") {
                  delete_wfl0_workforce_utilization(where: {timestamp: {_gte: $timestamp}}) {
                    affected_rows
                  }
                }
        """)
    await send_mutation(wfl0_workforce_utilization_table, params_delete)
    for date, workforce_utilization_dict in workforce_utilization_dict.items():
        variable_name = f"workforce_availability_dict_{date}"
        individual_workforce_utilization_dict[variable_name] = workforce_utilization_dict
        # Integrate the two lines here
        formatted_data = [item[1] for item in workforce_utilization_dict.items()]
        formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
        # print(formatted_data)
        mutation_objects = formatted_data
        wfl0_workforce_utilization_table = gql(
            """
                mutation MyMutation($objects: [wfl0_workforce_utilization_insert_input!] = {}) {
                      insert_wfl0_workforce_utilization(objects: $objects) {
                        affected_rows
                      }
                    }

            """
        )
        params = {"objects": mutation_objects}
        await send_mutation(wfl0_workforce_utilization_table, params)

    return {
        "status": "SUCCESS",
    }




@app.post("/daily_workforce_utilization")
async def dailyWorkforceUtilization():
    try:
        variables = {"timestamp": midnight_ts}
        print(variables)
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0WorkforceUtilization(variable))
        if df.empty:
            return {"status": "Required Data is not available"}


        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}

        workforce_utilization_dict_day_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_utilization_dict_day_time[department_id] = {}

            for day, day_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('D')):
                workforce_dict = {
                    'date': day_group['date'].iloc[-1],
                    'timestamp': day_group['timestamp'].iloc[-1],
                    'tenantid': day_group['tenantid'].iloc[-1],
                    'org_id': day_group['org_id'].iloc[-1],
                    'unit_id': day_group['unit_id'].iloc[-1],
                    'department_id': department_id,
                    'shift': day_group['shift'].iloc[-1],
                    'operator_id': day_group['operator_id'].iloc[-1],
                    'machineid': day_group['machineid'].iloc[-1],
                    'edgeid': day_group['edgeid'].iloc[-1],
                    'proximity_in_time': day_group['proximity_in_time'].sum(),
                    'total_run_time': day_group['total_run_time'].sum(),
                    'workforce_utilization_per': (
                        day_group['workforce_utilization_per'].mean(skipna=True)) if not np.isnan(
                        day_group['workforce_utilization_per'].mean(skipna=True)) else None
                }

                workforce_utilization_dict_day_time[department_id][day] = workforce_dict
        individual_workforce_utilization_dict_day_time = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_workforce_utilization_daily_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_utilization_daily(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_utilization_daily_table, params_delete)

        for department_id, workforce_dict_row in workforce_utilization_dict_day_time.items():
            variable_name = f"worforce_utilization_daily_{department_id}"
            individual_workforce_utilization_dict_day_time[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            # print(formatted_data)

            mutation_objects = formatted_data
            wfl0_workforce_utilization_daily_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_utilization_daily_insert_input!] = {}) {
                      insert_wfl0_workforce_utilization_daily(objects: $objects) {
                        affected_rows
                      }
                    }

                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_utilization_daily_table, params)
    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }


@app.post("/weekly_workforce_utilization")
async def weeklyWorkforceUtilization():
    try:
        variables = {"timestamp": getBefore_n_DaysEpochTimestamp(7)}
        print(getBefore_n_DaysEpochTimestamp(7))

       # variables = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0WorkforceUtilization(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}

        workforce_utilization_dict_week_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_utilization_dict_week_time[department_id] = {}

            for week, week_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('W')):
                workforce_dict = {
                    'start_date': week.start_time.strftime("%Y-%m-%d"),
                    'end_date': week.end_time.strftime("%Y-%m-%d"),
                    'timestamp': week_group['timestamp'].iloc[-1],
                    'tenantid': week_group['tenantid'].iloc[-1],
                    'org_id': week_group['org_id'].iloc[-1],
                    'unit_id': week_group['unit_id'].iloc[-1],
                    'operator_id': week_group['operator_id'].iloc[-1],
                    'machineid': week_group['machineid'].iloc[-1],
                    'edgeid': week_group['edgeid'].iloc[-1],
                    'department_id': department_id,
                    'shift': week_group['shift'].iloc[-1],
                    'proximity_in_time': week_group['proximity_in_time'].sum(),
                    'total_run_time': week_group['total_run_time'].sum(),
                    'workforce_utilization_per': (
                        week_group['workforce_utilization_per'].mean(skipna=True)) if not np.isnan(
                        week_group['workforce_utilization_per'].mean(skipna=True)) else None
                }

                workforce_utilization_dict_week_time[department_id][week] = workforce_dict
        individual_workforce_utilization_dict_week_time = {}
        params_delete = {"timestamp": getBefore_n_DaysEpochTimestamp(7)}
        wfl0_workforce_utilization_weekly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_utilization_weekly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_utilization_weekly_table, params_delete)

        for department_id, workforce_dict_row in workforce_utilization_dict_week_time.items():
            variable_name = f"worforce_utilization_daily_{department_id}"
            individual_workforce_utilization_dict_week_time[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            # print(formatted_data)

            mutation_objects = formatted_data
            wfl0_workforce_utilization_weekly_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_utilization_weekly_insert_input!] = {}) {
                      insert_wfl0_workforce_utilization_weekly(objects: $objects) {
                        affected_rows
                      }
                    }

                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_utilization_weekly_table, params)
    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }


@app.post("/monthly_workforce_utilization")
async def monthlyWorkforceUtilization():
    try:
       # variables = {"timestamp": 1704689530000}
        timestamp = get_start_epoch_time_of_yesterdays_month()
        print(timestamp)
        variables = {"timestamp": timestamp}
        df = pd.DataFrame(await getWfl0WorkforceUtilization(variables))
        if df.empty:
            return {"status": "Required Data is not available"}


        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}

        workforce_utilization_dict_month_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_utilization_dict_month_time[department_id] = {}

            for month, month_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('M')):
                workforce_dict = {
                    'start_date': month.start_time.strftime("%Y-%m-%d"),
                    'end_date': month.end_time.strftime("%Y-%m-%d"),
                    'timestamp': month_group['timestamp'].iloc[-1],
                    'tenantid': month_group['tenantid'].iloc[-1],
                    'org_id': month_group['org_id'].iloc[-1],
                    'unit_id': month_group['unit_id'].iloc[-1],
                    'department_id': department_id,
                    'operator_id': month_group['operator_id'].iloc[-1] if not pd.isna(month_group['operator_id'].iloc[-1]) else None,
                    'machineid': month_group['machineid'].iloc[-1] if not pd.isna(month_group['machineid'].iloc[-1]) else None,
                    'edgeid': month_group['edgeid'].iloc[-1],
                    'shift': month_group['shift'].iloc[-1],
                    'proximity_in_time': month_group['proximity_in_time'].sum(),
                    'total_run_time': month_group['total_run_time'].sum(),
                    'workforce_utilization_per': (month_group['workforce_utilization_per'].mean(skipna=True)) if not np.isnan(
                        month_group['workforce_utilization_per'].mean(skipna=True)) else None
                }

                workforce_utilization_dict_month_time[department_id][month] = workforce_dict
        individual_workforce_utilization_dict_month_time = {}
        params_delete = {"timestamp": timestamp}
        wfl0_workforce_utilization_monthly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_utilization_monthly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_utilization_monthly_table, params_delete)

        for department_id, workforce_dict_row in workforce_utilization_dict_month_time.items():
            variable_name = f"worforce_utilization_monthly_{department_id}"
            individual_workforce_utilization_dict_month_time[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            # print(formatted_data)

            mutation_objects = formatted_data
            wfl0_workforce_utilization_monthly_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_utilization_monthly_insert_input!] = {}) {
                      insert_wfl0_workforce_utilization_monthly(objects: $objects) {
                        affected_rows
                      }
                    }

                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_utilization_monthly_table, params)
    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }


@app.post("/yearly_workforce_utilization")
async def yearlyWorkforceUtilization():
    try:
        #variables = {"timestamp": 1704689530000}
        timestamp = get_start_epoch_time_of_yesterdays_month()
        print(timestamp)
        variables = {"timestamp": timestamp}
        df = pd.DataFrame(await getWfl0WorkforceUtilization(variables))
        if df.empty:
            return {"status": "Required Data is not available"}


        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}

        workforce_utilization_dict_yearly_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_utilization_dict_yearly_time[department_id] = {}

            for year, year_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('Y')):
                workforce_dict = {
                    'start_date': year.start_time.strftime("%Y-%m-%d"),
                    'end_date': year.end_time.strftime("%Y-%m-%d"),
                    'timestamp': year_group['timestamp'].iloc[-1],
                    'tenantid': year_group['tenantid'].iloc[-1],
                    'org_id': year_group['org_id'].iloc[-1],
                    'unit_id': year_group['unit_id'].iloc[-1],
                    'department_id': department_id,
                    'shift': year_group['shift'].iloc[-1],
                    'operator_id': year_group['operator_id'].iloc[-1] if not pd.isna(year_group['operator_id'].iloc[-1]) else None,
                    'machineid': year_group['machineid'].iloc[-1] if not pd.isna(year_group['machineid'].iloc[-1]) else None,
                    'edgeid': year_group['edgeid'].iloc[-1],
                    'proximity_in_time': year_group['proximity_in_time'].sum(),
                    'total_run_time': year_group['total_run_time'].sum() ,
                    'workforce_utilization_per': (year_group['workforce_utilization_per'].mean(skipna=True)) if not np.isnan(
                        year_group['workforce_utilization_per'].mean(skipna=True)) else None
                }

                workforce_utilization_dict_yearly_time[department_id][year] = workforce_dict
        individual_workforce_utilization_dict_yearly_time= {}
        params_delete = {"timestamp": timestamp}
        wfl0_workforce_utilization_yearly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_utilization_yearly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_utilization_yearly_table, params_delete)

        for department_id, workforce_dict_row in workforce_utilization_dict_yearly_time.items():
            variable_name = f"worforce_utilization_daily_{department_id}"
            individual_workforce_utilization_dict_yearly_time[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            # print(formatted_data)

            mutation_objects = formatted_data
            wflo_workforce_availability_yearly_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_utilization_yearly_insert_input!] = {}) {
                      insert_wfl0_workforce_utilization_yearly(objects: $objects) {
                        affected_rows
                      }
                    }

                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_availability_yearly_table, params)
    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }


@app.post("/workforce_utilization_by_experience")
async def WorkforceUtilizationByExperiene():
    try:
        #variables = {"timestamp": 1704689530000}
        variables = {"timestamp": midnight_ts}
        df = pd.DataFrame(await getWfl0WorkforceUtilization(variables))
        if df.empty:
            return {"status": "Required Data is not available"}

        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('machineid')}

        workforce_utilization_by_experience_dict = {}

        for machineid, df_machine in separate_dfs.items():

            df_machine = df_machine.sort_values(by='timestamp', ascending=True)

            workforce_utilization_by_experience_dict[machineid] = {}

            for (date, experience), df_group in df_machine.groupby([df_machine['timestamp1'].dt.date, df_machine['experience']]):

                workforce_dict = {
                    'date': df_group['date'].iloc[-1],
                    'timestamp': df_group['timestamp'].iloc[-1],
                    'tenantid': df_group['tenantid'].iloc[-1],
                    'org_id': df_group['org_id'].iloc[-1],
                    'unit_id': df_group['unit_id'].iloc[-1],
                    'department_id': df_group['department_id'].iloc[-1],
                    'experience': experience,
                    'machineid': df_group['machineid'].iloc[-1],
                    'edgeid': df_group['edgeid'].iloc[-1],
                    'proximity_in_time': (df_group['proximity_in_time'].mean(skipna=True)) if not np.isnan(
                        df_group['proximity_in_time'].mean(skipna=True)) else None,
                    'total_run_time': (df_group['total_run_time'].mean(skipna=True)) if not np.isnan(
                        df_group['total_run_time'].mean(skipna=True)) else None,
                    'workforce_utilization_per': (df_group['workforce_utilization_per'].mean(skipna=True)) if not np.isnan(
                        df_group['workforce_utilization_per'].mean(skipna=True)) else None
                }

                workforce_utilization_by_experience_dict[machineid][experience] = workforce_dict
               # print(workforce_utilization_by_experience_dict)
        individual_workforce_utilization_by_experience_dict = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_workforce_utilization_by_experience_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_utilization_by_experience(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_utilization_by_experience_table, params_delete)

        for machineid, workforce_dict_row in workforce_utilization_by_experience_dict.items():
            variable_name = f"worforce_utilization_experiencewise_{machineid}"
            individual_workforce_utilization_by_experience_dict[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            # print(formatted_data)
            mutation_objects = formatted_data
            wfl0_workforce_utilization_daily_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_utilization_by_experience_insert_input!] = {}) {
                      insert_wfl0_workforce_utilization_by_experience(objects: $objects) {
                        affected_rows
                      }
                    }                  
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_utilization_daily_table, params)
    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }
