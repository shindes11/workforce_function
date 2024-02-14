import numpy as np
from fastapi import FastAPI, Request
import requests
from fastapi.encoders import jsonable_encoder
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import uvicorn
import datetime as dt
import time
import math
import pandas as pd
import datetime
import statistics
import json
from app.wfl0_function import get_realtime_millisecond_timestamp, convert_to_json_serializable, getBefore_n_HoursEpochTimestamp,getBefore_n_DaysEpochTimestamp, \
    getBefore_n_MonthsEpochTimestamp, get_one_hour_before_realtime_millisecond_timestamp, assignOperatorToTag,  getProximityInTimeTotal, getProximityOutTimeTotal,\
    create_shift_dict, AssignShiftId, addShiftStartAndShiftEnd,calculateEffectiveProximityTime,calculateProximityLossTimePercentage,get_start_epoch_time_of_yesterdays_month,\
    getTotalWorkforce,  workforceAvailabilityPercentage,  assign_org_unit_dept, getTotalavailableWorkforce, AvailableWorkforceByJobRole, \
    AvailableWorkforceBySkill, getUnavailableWorkforce, getUnavailableWorkforcePer, getUnavailableWorkforceByJobRole, \
    getAvailableWorkforceByJobRolePer, getUnAvailableWorkforceByJobRolePer, \
    getUnavailableWorkforceByskill, getavailableWorkforceBySkillPer, getUnavailableWorkforceBySkillPer,getOperatorSkills,fetchRunTime,calculateWorkforceUtilization ,getTotalavailableWorkforce,AvailableWorkforceByJobRole,\
AvailableWorkforceBySkill,getOperatorJobRole,getOperatorSalary, getOperatorGender, getOperatorCertification, getOperatorTotalExperience,\
    getOperatorEducation, getOperatorMaritalStatus


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
        # print(result)
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
        # print(params)
        result = await client.execute_async(mutation, variable_values=params)
        print(result)
    except Exception as e:
        print(e)


app = FastAPI()


@app.get("/")
async def root():
    print("Hello")
    return {"message": "hello : HUMAC"}


def get_midnight_epoch_timestamp():
    current_datetime = datetime.datetime.now()
    midnight_datetime = datetime.datetime.combine(current_datetime.date(), datetime.time.min)
    midnight_timestamp = int(midnight_datetime.timestamp() * 1000)
    return midnight_timestamp


midnight_ts = get_midnight_epoch_timestamp()
# print(midnight_ts)
realtime_millisecond_timestamp = get_realtime_millisecond_timestamp()
# print(realtime_millisecond_timestamp)
one_hour_before_timestamp = get_one_hour_before_realtime_millisecond_timestamp()


# print(one_hour_before_timestamp)


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


async def getwfl0OeratorProximityLogDf(variable):
    query2 = gql("""
    query MyQuery($timestamp: bigint = "") {
          wfl0_operator_proximity_log(where: {timestamp: {_gte: $timestamp}}) {
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


async def getActualWorkforcedict():
    query2 = gql("""
        query MyQuery {
            tenant_employees(where: {tenantid: {_is_null: false}}) {
                employee_id
                department
                tenantid

            }
        }          
    """)
    data = await get_data1(query2)
    return data['tenant_employees']


async def getWdmWorforceAvailability(variable):
    query = gql("""
    query MyQuery($timestamp: bigint = "") {
          wdm_workforce_availability(where: {timestamp: {_gte: $timestamp}}) {
            available_workforce
            available_workforce_by_job_role
            available_workforce_by_skill
            available_workforce_per_by_job_role
            available_workforce_per_by_skill
            date
            department_id
            job_role
            org_id
            shift
            skill
            tenantid
            timestamp
            total_workforce
            unavailable_workforce
            unavailable_workforce_by_skill
            unavailable_workforce_per
            unavailable_workforce_per_by_job_role
            unavailable_workforce_per_by_skill
            unavailable_workorce_by_job_role
            unit_id
            workforce_availability_per
          }
        }    
    """)
    data = await get_data(query, variable)
    return data['wdm_workforce_availability']

async def getBatchDeatilsDict():
    query = gql("""
        query MyQuery {
              oa_batch_details {
                batch_start_date
                batch_start_time
                end_customer_name
                part_id
                machineid
                operator_id
                tenantid
              }
            }
    """)
    data = await get_data1(query)
    return data['oa_batch_details']
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


async def getshiftStartshiftEnd():
    query2 = gql("""
    query get_shift_data {
        tnt_work_schedule_masters {
            tenantid,
            shift_name,
            shift_end_time,
            shift_start_time
            }
        }
    """)
    data = await get_data1(query2)
    return data['tnt_work_schedule_masters']


async def tenant_employee_dict():
    query2 = gql("""
        query MyQuery {
          tenant_employees(where: {tenantid: {_is_null: false}}) {
            certification
            education
            employee_id
            gender
            marital_status
            monthly_ctc
            role_id
            tenantid
            total_exp_yr
            job_role_id
          }
        }

            """)
    data = await get_data1(query2)
    return data['tenant_employees']


async def getActualWorkforcedict():
    query2 = gql("""
        query MyQuery {
            tenant_employees(where: {tenantid: {_is_null: false}}) {
                employee_id
                department
                tenantid

            }
        }          
    """)
    data = await get_data1(query2)
    return data['tenant_employees']


async def getOpertorJobRoleDict():
    query2 = gql("""
                    query MyQuery {
                  job_role_master {
                    job_id
                    job_role
                  }
                }
            """)
    data = await get_data1(query2)
    return data['job_role_master']


async def getoperatorSkillDict():
    query = gql("""
    query MyQuery {
              operator_production_plan_assignment {
                operator_id
                skill
                tenantid
              }
            }

    """)
    data = await get_data1(query)
    return data['operator_production_plan_assignment']


async def getConfigeCodeDict():
    query = gql("""

    query MyQuery {
      config_code_master {
        code_id
        code
      }
    }

    """)
    data = await get_data1(query)
    return data['config_code_master']
async def getOperatorMachineAvailability(variable):
    query = gql("""
    query MyQuery($timestamp: bigint = "") {
          wfl0_operator_machine_availability(where: {timestamp: {_gte: $timestamp}}) {
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


@app.post("/Operator Proximity Log")
async def operatorProximityLog():
    try:

        variables = {"ts": midnight_ts}
        #print( "ts",getBefore_n_HoursEpochTimestamp(12))

        df = pd.DataFrame(await getProximityData(variables))
        #df = pd.read_csv("/Users/HALCYON007/proximity_feb9tofeb10.csv")
        #df = pd.read_csv("/Users/HALCYON007/proximity_feb7to9.csv")
        #df = pd.read_csv("/Users/HALCYON007/proximity_feb6.csv")
         #df = pd.read_csv("/Users/HALCYON007/proximity_jan20tojan24.csv")
        #df = pd.read_csv("/Users/HALCYON007/proximity_jan25to26.csv")
        #df = pd.read_csv("/Users/HALCYON007/proximity_feb10tofeb13.csv")
        #df = pd.read_csv("/Users/HALCYON007/proximity_today.csv")

        if df.empty:
            return {"status": "Required Data is not available"}

        #print(df)
        batch_details_dict = await getBatchDeatilsDict()
        operator_tag_dict = await getEmpTagAssignData()
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms", origin='unix')
        tenant_emp_dict = await tenant_employee_dict()
        df2 = pd.DataFrame(await getoperatorSkillDict())
        job_role_dict = await getOpertorJobRoleDict()
        confige_code_dict = await getConfigeCodeDict()
        operator_proximity_dict = []
        for index, row in df.iterrows():
            operator_id = assignOperatorToTag(row['tenantid'], row['tagid'], operator_tag_dict)
            job_role = getOperatorJobRole(operator_id, tenant_emp_dict, job_role_dict)
            salary = getOperatorSalary(tenant_emp_dict, operator_id, row['tenantid'])
            gender = getOperatorGender(tenant_emp_dict, operator_id, row['tenantid'])
            certification = getOperatorCertification(tenant_emp_dict, operator_id, row['tenantid'])
            experience = getOperatorTotalExperience(tenant_emp_dict, operator_id, row['tenantid'])
            skill = getOperatorSkills(df2, row['tenantid'], operator_id)
            Education = getOperatorEducation(tenant_emp_dict, confige_code_dict, operator_id, row['tenantid'])
            marital_status = getOperatorMaritalStatus(tenant_emp_dict, confige_code_dict, operator_id, row['tenantid'])
            operator_proximity_log_dict = {
                'date': row['timestamp'].date().strftime("%Y-%m-%d"),
                'timestamp': row['ts'],
                'tenantid': row['tenantid'],
                'machineid': row['machineid'],
                'edgeid': row['edgeid'],
                'tagid': row['tagid'],
                'operator_id': operator_id,
                'job_role': job_role,
                'salary': salary,
                'gender': gender,
                'skill': skill,
                'certification': certification,
                'experience': experience,
                'education': Education,
                'marital_status': marital_status
            }
            #print(operator_proximity_log_dict)
            operator_proximity_dict.append(operator_proximity_log_dict)
        params_delete = {"ts": midnight_ts}
        wfl0_operator_proximity_log_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_operator_proximity_log(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_operator_proximity_log_table, params_delete)
        formatted_data = json.loads(json.dumps(operator_proximity_dict, default=convert_to_json_serializable))
        mutation_objects = formatted_data

        wfl0_proximity_log_table = gql(
            """
            mutation MyMutation($objects: [wfl0_operator_proximity_log_insert_input!] = {}) {
                insert_wfl0_operator_proximity_log(objects: $objects) {
                    affected_rows
                }
            }
            """)

        params = {"objects": mutation_objects}
        await send_mutation(wfl0_proximity_log_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }






@app.post("/workforce_availability")
async def workforceAvailability():
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

        variables = {"timestamp": getBefore_n_HoursEpochTimestamp(1)}
       # df = pd.read_csv("/Users/HALCYON007/wfl0_operator_proximity_log_202402132002.csv")
        df = pd.DataFrame(await getWdmWorforceAvailability(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit="ms", origin='unix')
        shift_id_df = pd.DataFrame(await getShiftDict())
        shift_id_dict = create_shift_dict(shift_id_df)
        df["shift_id"] = AssignShiftId(df, shift_id_dict)
        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)


        tenant_emp_dict = await getActualWorkforcedict()
        # seperated_dfs
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        avg_workforce_dict = {}
        for deparment_id, df_dept in separate_dfs.items():

            # Sort the DataFrame by timestamp
            df_tenant = df_dept.sort_values(by='timestamp', ascending=True)

            # Initialize the inner dictionary for the current machine_id
            avg_workforce_dict[deparment_id] = {}

            for (date, hour), hour_group in df_tenant.groupby([df_tenant['timestamp'].dt.date, df_tenant['timestamp'].dt.hour]):
                planned_workforce = getTotalWorkforce(tenant_emp_dict, deparment_id)
                available_workforce = getTotalavailableWorkforce(hour_group)
                workforce_availability_per = workforceAvailabilityPercentage(planned_workforce, available_workforce)
                timestamp = hour_group['timestamp'].iloc[-1]
                epoch_timestamp = int(timestamp.timestamp() * 1000)
                workforce_count_dict = {
                    'date': date,
                    'timestamp':epoch_timestamp,
                    'tenantid': hour_group['tenantid'].iloc[-1],
                    'org_id': hour_group['org_id'].iloc[-1],
                    'unit_id': hour_group['unit_id'].iloc[-1],
                    'department_id': hour_group['department_id'].iloc[-1],
                    'shift': hour_group['shift_id'].iloc[-1],
                    'planned_workforce': planned_workforce,
                    'available_workforce': available_workforce,
                    'workforce_availability_per': workforce_availability_per
                }

                avg_workforce_dict[deparment_id][hour] = workforce_count_dict
        params_delete = {"timestamp": getBefore_n_HoursEpochTimestamp(1)}
        wdm_workforce_availability_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wdm_workforce_availability(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wdm_workforce_availability_table, params_delete)
        for tenant_id, workforce_count_dict in avg_workforce_dict.items():
            formatted_data = [item for item in workforce_count_dict.values()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            # print(formatted_data)
            mutation_objects = formatted_data
            wfl0_workforce_available_table = gql(
                """
               mutation MyMutation($objects: [wfl0_workforce_availability_insert_input!] = {}) {
              insert_wfl0_workforce_availability(objects: $objects) {
                affected_rows
              }
            }""")
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_available_table, params)
        return "Success!"

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/wfl0_workforce_availability_by_job-role")
async def workforceAvailabilityJobRole():
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
        variables = {"timestamp": getBefore_n_HoursEpochTimestamp(1)}
        #variables = {"timestamp": 1707814800000}
        df = pd.DataFrame(await getwfl0OeratorProximityLogDf(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        # Ensure 'timestamp1' column has datetime-like values
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit="ms", origin='unix')
        shift_id_df = pd.DataFrame(await getShiftDict())
        shift_id_dict = create_shift_dict(shift_id_df)
        df["shift_id"] = AssignShiftId(df, shift_id_dict)
        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)


        tenant_emp_dict = await getActualWorkforcedict()

        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        avg_workforce_dict = {}
        for deparment_id, df_dept in separate_dfs.items():
            df_tenant = df_dept.sort_values(by='timestamp', ascending=True)
            avg_workforce_dict[deparment_id] = {}

            for (date, hour), hour_group in df_dept.groupby([df_dept['timestamp'].dt.date, df_dept['timestamp'].dt.hour]):
                planned_workforce = getTotalWorkforce(tenant_emp_dict, deparment_id)
                available_workforce_job_role = AvailableWorkforceByJobRole(hour_group, hour_group['job_role'].iloc[-1])
                workforce_availability_per = workforceAvailabilityPercentage(planned_workforce,available_workforce_job_role)
                timestamp = hour_group['timestamp'].iloc[-1]
                epoch_timestamp = int(timestamp.timestamp() * 1000)
                workforce_count_dict = {
                    'date': date,
                    'timestamp': epoch_timestamp,
                    'tenantid': hour_group['tenantid'].iloc[-1],
                    'org_id': hour_group['org_id'].iloc[-1],
                    'unit_id': hour_group['unit_id'].iloc[-1],
                    'department_id': hour_group['department_id'].iloc[-1],
                    'shift': hour_group['shift_id'].iloc[-1],
                    'job_role': hour_group['job_role'].iloc[-1],
                    'total_workforce': planned_workforce,
                    'available_workforce': available_workforce_job_role,
                    'workforce_availability_per': workforce_availability_per,
                    'unavailable_workforce': (planned_workforce - available_workforce_job_role),
                    'unavailable_workforce_per': (planned_workforce - available_workforce_job_role) * 100 / planned_workforce if planned_workforce else 0

                }
                #print(workforce_count_dict)
                avg_workforce_dict[deparment_id][hour] = workforce_count_dict
        params_delete = {"timestamp": getBefore_n_HoursEpochTimestamp(1)}
        wfl0_workforce_availability_by_job_role_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_availability_by_job_role(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_workforce_availability_by_job_role_table, params_delete)

        for deparment_id, workforce_count_dict in avg_workforce_dict.items():
            formatted_data = [item for item in workforce_count_dict.values()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            # print(formatted_data)
            mutation_objects = formatted_data
            wfl0_workforce_availability_by_job_role_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_availability_by_job_role_insert_input!] = {}) {
                  insert_wfl0_workforce_availability_by_job_role(objects: $objects) {
                    affected_rows
                  }
                }
              """)
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_availability_by_job_role_table, params)
        return "Success!"

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/wfl0_workforce_availability_by_skill")
async def workforceAvailabilityBySkill():
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
        variables = {"timestamp": getBefore_n_HoursEpochTimestamp(1)}
        #variables = {"timestamp": 1704707674000}
        df = pd.DataFrame(await getwfl0OeratorProximityLogDf(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        # Ensure 'timestamp1' column has datetime-like values
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit="ms", origin='unix')
        shift_id_df = pd.DataFrame(await getShiftDict())
        shift_id_dict = create_shift_dict(shift_id_df)
        df["shift_id"] = AssignShiftId(df, shift_id_dict)
        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)


        tenant_emp_dict = await getActualWorkforcedict()

        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}
        avg_workforce_dict = {}
        for deparment_id, df_dept in separate_dfs.items():
            df_dept = df_dept.sort_values(by='timestamp', ascending=True)
            avg_workforce_dict[deparment_id] = {}

            for (date, hour ,skill), hour_group in df_dept.groupby([df_dept['timestamp'].dt.date, df_dept['timestamp'].dt.hour,df_dept['skill']]):
                planned_workforce = getTotalWorkforce(tenant_emp_dict, deparment_id)
                available_workforce_By_skill = AvailableWorkforceBySkill(hour_group, skill)
                workforce_availability_per = workforceAvailabilityPercentage(planned_workforce,available_workforce_By_skill)
                timestamp = hour_group['timestamp'].iloc[-1]
                epoch_timestamp = int(timestamp.timestamp() * 1000)
                workforce_count_dict = {
                    'date': date,
                    'timestamp': epoch_timestamp,
                    'tenantid': hour_group['tenantid'].iloc[-1],
                    'org_id': hour_group['org_id'].iloc[-1],
                    'unit_id': hour_group['unit_id'].iloc[-1],
                    'department_id': hour_group['department_id'].iloc[-1],
                    'shift': hour_group['shift_id'].iloc[-1],
                    'skill': skill,
                    'total_workforce': planned_workforce,
                    'available_workforce': available_workforce_By_skill,
                    'workforce_availability_per': workforce_availability_per,
                    'unavailable_workforce': (planned_workforce - available_workforce_By_skill),
                    'unavailable_workforce_per': (planned_workforce - available_workforce_By_skill) * 100 / planned_workforce if planned_workforce else 0

                }
                # print(workforce_count_dict)
                avg_workforce_dict[deparment_id][hour] = workforce_count_dict
        params_delete = {"timestamp": getBefore_n_HoursEpochTimestamp(1)}
        wdm_workforce_availability_shiftwise_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_workforce_availability_by_skill(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wdm_workforce_availability_shiftwise_table, params_delete)
        for deparment_id, workforce_count_dict in avg_workforce_dict.items():
            formatted_data = [item for item in workforce_count_dict.values()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            # print(formatted_data)
            mutation_objects = formatted_data
            wfl0_workforce_availability_by_skill_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_availability_by_skill_insert_input!] = {}) {
                  insert_wfl0_workforce_availability_by_skill(objects: $objects) {
                    affected_rows
                  }
                }

              """)
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_availability_by_skill_table, params)
        return "Success!"

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/wdm_workforce_availability")
async def shiftwiseworkforceAvailability():
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
        s2 = await get_data1(query2)  # tnt_org_machine_assignment to assign org_id,department_id, unit_id
        variables = {"timestamp":midnight_ts}

       # variables = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getwfl0OeratorProximityLogDf(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit="ms", origin='unix')
        shift_id_df = pd.DataFrame(await getShiftDict())
        shift_id_dict = create_shift_dict(shift_id_df)
        df["shift_id"] = AssignShiftId(df, shift_id_dict)
        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)
        tenant_emp_dict = await getActualWorkforcedict()
        #print(tenant_emp_dict)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}

        avg_workforce_dict = {}
        for deparment_id, df_dept in separate_dfs.items():

            # Sort the DataFrame by timestamp
            df_tenant = df_dept.sort_values(by='timestamp', ascending=True)

            # Initialize the inner dictionary for the current machine_id
            avg_workforce_dict[deparment_id] = {}

            for (date, hour), hour_group in df_tenant.groupby([df_tenant['timestamp'].dt.date, df_tenant['timestamp'].dt.hour]):
                total_workforce = getTotalWorkforce(tenant_emp_dict, deparment_id)
                #print(deparment_id,total_workforce)
                available_workforce = getTotalavailableWorkforce(hour_group)
                unavailable_workforce = getUnavailableWorkforce(total_workforce, available_workforce)
                unavailable_workforce_per = getUnavailableWorkforcePer(total_workforce, unavailable_workforce)
                workforce_availability_per = workforceAvailabilityPercentage(total_workforce, available_workforce)
                available_workforce_job_role = AvailableWorkforceByJobRole(hour_group, hour_group['job_role'].iloc[-1])
                unavailable_workforce_job_role = getUnavailableWorkforceByJobRole(total_workforce,available_workforce_job_role)
                available_workforce_per_by_job_role = getAvailableWorkforceByJobRolePer(total_workforce,available_workforce_job_role)
                unavailable_workforce_per_by_job_role = getUnAvailableWorkforceByJobRolePer(total_workforce, unavailable_workforce_job_role)
                available_workforce_By_skill = AvailableWorkforceBySkill(hour_group,hour_group['skill'].iloc[-1])
                unavailable_workforce_By_skill = getUnavailableWorkforceByskill(total_workforce,available_workforce_By_skill)
                available_workforce_per_by_skill = getavailableWorkforceBySkillPer(total_workforce,available_workforce_By_skill)
                unavailable_workforce_per_by_skill = getUnavailableWorkforceBySkillPer(total_workforce,unavailable_workforce_By_skill)
                timestamp = hour_group['timestamp'].iloc[-1]
                epoch_timestamp = int(timestamp.timestamp() * 1000)

                # Create a new dictionary to hold the data for this hour
                hour_data = {
                    'date': hour_group['timestamp'].dt.date.iloc[-1].strftime("%Y-%m-%d"),
                    'timestamp': epoch_timestamp,
                    'tenantid': hour_group['tenantid'].iloc[-1],
                    'org_id': hour_group['org_id'].iloc[-1],
                    'unit_id': hour_group['unit_id'].iloc[-1],
                    'department_id': hour_group['department_id'].iloc[-1],
                    'shift': hour_group['shift_id'].iloc[-1],
                    'skill': hour_group['skill'].iloc[-1],
                    'job_role': hour_group['job_role'].iloc[-1],
                    'total_workforce': total_workforce,
                    'available_workforce': available_workforce,
                    'unavailable_workforce': unavailable_workforce,
                    'workforce_availability_per': workforce_availability_per,
                    'unavailable_workforce_per': unavailable_workforce_per,
                    'available_workforce_by_job_role': available_workforce_job_role,
                    'available_workforce_per_by_job_role': available_workforce_per_by_job_role,
                    'unavailable_workorce_by_job_role': unavailable_workforce_job_role,
                    'unavailable_workforce_per_by_job_role': unavailable_workforce_per_by_job_role,
                    'available_workforce_by_skill': available_workforce_By_skill,
                    'unavailable_workforce_by_skill': unavailable_workforce_By_skill,
                    'available_workforce_per_by_skill': available_workforce_per_by_skill,
                    'unavailable_workforce_per_by_skill': unavailable_workforce_per_by_skill
                }

                # Add the hour_data to the workforce_availability_dict under the tenant_id and hour
                avg_workforce_dict[deparment_id][(date, hour)] = hour_data
        individual_workforce_utilization_dict = {}
        params_delete = {"timestamp":midnight_ts}
        wdm_workforce_availability_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wdm_workforce_availability(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wdm_workforce_availability_table, params_delete)
        for tenant_id, workforce_utilization_dict in avg_workforce_dict.items():
            variable_name = f"workforce_availability_dict_{tenant_id}"
            individual_workforce_utilization_dict[variable_name] = workforce_utilization_dict
            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_utilization_dict.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            wfl0_workforce_availability_shiftwise_table = gql(
                """
                mutation MyMutation($objects: [wdm_workforce_availability_insert_input!] = {}) {
                      insert_wdm_workforce_availability(objects: $objects) {
                        affected_rows
                      }
                    }
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_availability_shiftwise_table, params)
        return "Success!"

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }



@app.post("/wdm_workforce_availability_shiftwise")
async def wdm_workforce_availability_shiftwise():
    try:
        variables = {"timestamp": midnight_ts}
        #print(variables)
        #variables = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWdmWorforceAvailability(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp_1'] = pd.to_datetime(df['timestamp'], unit="ms")
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp_1', ascending=True) for name, group in
                        df.groupby('department_id')}
        wdm_dict_day_time = {}

        for department_id, df_machine in separate_dfs.items():
            wdm_dict_day_time[department_id] = {}

            for (day, shift), day_group in df_machine.groupby([df_machine['timestamp_1'].dt.to_period('D'), 'shift']):
                datamart_dict_individual = {
                    'date': day_group['date'].iloc[-1],
                    'timestamp': day_group['timestamp'].iloc[-1],
                    'tenantid': day_group['tenantid'].iloc[-1],
                    'org_id': day_group['org_id'].iloc[-1],
                    'unit_id': day_group['unit_id'].iloc[-1],
                    'department_id': day_group['department_id'].iloc[-1],
                    'shift': shift,
                    'skill': day_group['skill'].iloc[-1],
                    'job_role': day_group['job_role'].iloc[-1],
                    'total_workforce': math.ceil(day_group['total_workforce'].sum() / len(day_group['total_workforce'])),
                    'available_workforce': math.ceil(
                        day_group['available_workforce'].sum() / len(day_group['available_workforce'])),
                    'unavailable_workforce': math.ceil(
                        day_group['unavailable_workforce'].sum() / len(day_group['unavailable_workforce'])),
                    'workforce_availability_per': (
                        day_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(
                        day_group['workforce_availability_per'].mean(skipna=True)) else None,
                    'unavailable_workforce_per': (day_group['unavailable_workforce_per'].mean(skipna=True)) if not np.isnan(
                        day_group['unavailable_workforce_per'].mean(skipna=True)) else None,
                    'available_workforce_by_job_role': math.ceil(day_group['available_workforce_by_job_role'].sum() / len(
                        day_group['available_workforce_by_job_role'])),
                    'available_workforce_per_by_job_role': (
                        day_group['available_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        day_group['available_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'unavailable_workorce_by_job_role': math.ceil(day_group['unavailable_workorce_by_job_role'].sum() / len(
                        day_group['unavailable_workorce_by_job_role'])),
                    'unavailable_workforce_per_by_job_role': (
                        day_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        day_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'available_workforce_by_skill': math.ceil(
                        day_group['available_workforce_by_skill'].sum() / len(day_group['available_workforce_by_skill'])),
                    'unavailable_workforce_by_skill': math.ceil(day_group['unavailable_workforce_by_skill'].sum() / len(
                        day_group['unavailable_workforce_by_skill'])),
                    'available_workforce_per_by_skill': (
                        day_group['available_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        day_group['available_workforce_per_by_skill'].mean(skipna=True)) else None,
                    'unavailable_workforce_per_by_skill': (
                        day_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        day_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) else None,

                }
                wdm_dict_day_time[department_id][day] = datamart_dict_individual

        individual_datamart_day_time = {}
        params_delete = {"timestamp": midnight_ts}
        wdm_workforce_availability_shiftwise_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wdm_workforce_availability_shiftwise(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wdm_workforce_availability_shiftwise_table, params_delete)


        for department_id, wdm_dict_day_time_row in wdm_dict_day_time.items():
            variable_name = f"wdm_{department_id}"
            individual_datamart_day_time[variable_name] = wdm_dict_day_time_row

            formatted_data = [item[1] for item in wdm_dict_day_time_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            mutation_objects = formatted_data
            wdm_workforce_availability_shiftwise_table = gql(
                """
            mutation MyMutation($objects: [wdm_workforce_availability_shiftwise_insert_input!] = {}) {
              insert_wdm_workforce_availability_shiftwise(objects: $objects) {
                affected_rows
              }
            }

                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wdm_workforce_availability_shiftwise_table, params)

        return "Success!"

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/wdm_workforce_availability_daily")
async def wdm_workforce_availability_daily():
    try:
        variables = {"timestamp": midnight_ts}
        #variables = {"timestamp": 1704689530000}
        df = pd.DataFrame(await getWdmWorforceAvailability(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")

        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}

        workforce_dict_day_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_dict_day_time[department_id] = {}

            for day, day_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('D')):
                datamart_dict_individual = {
                    'date': day_group['date'].iloc[-1],
                    'timestamp': day_group['timestamp'].iloc[-1],
                    'tenantid': day_group['tenantid'].iloc[-1],
                    'org_id': day_group['org_id'].iloc[-1],
                    'unit_id': day_group['unit_id'].iloc[-1],
                    'department_id': day_group['department_id'].iloc[-1],
                    'shift': day_group['shift'].iloc[-1],
                    'skill': day_group['skill'].iloc[-1],
                    'job_role': day_group['job_role'].iloc[-1],
                    'total_workforce': math.ceil(day_group['total_workforce'].sum() / len(day_group['total_workforce'])),
                    'available_workforce': math.ceil(
                        day_group['available_workforce'].sum() / len(day_group['available_workforce'])),
                    'unavailable_workforce': math.ceil(
                        day_group['unavailable_workforce'].sum() / len(day_group['unavailable_workforce'])),
                    'workforce_availability_per': (
                        day_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(
                        day_group['workforce_availability_per'].mean(skipna=True)) else None,
                    'unavailable_workforce_per': (day_group['unavailable_workforce_per'].mean(skipna=True)) if not np.isnan(
                        day_group['unavailable_workforce_per'].mean(skipna=True)) else None,
                    'available_workforce_by_job_role': math.ceil(day_group['available_workforce_by_job_role'].sum() / len(
                        day_group['available_workforce_by_job_role'])),
                    'available_workforce_per_by_job_role': (
                        day_group['available_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        day_group['available_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'unavailable_workorce_by_job_role': math.ceil(day_group['unavailable_workorce_by_job_role'].sum() / len(
                        day_group['unavailable_workorce_by_job_role'])),
                    'unavailable_workforce_per_by_job_role': (
                        day_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        day_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'available_workforce_by_skill': math.ceil(
                        day_group['available_workforce_by_skill'].sum() / len(day_group['available_workforce_by_skill'])),
                    'unavailable_workforce_by_skill': math.ceil(day_group['unavailable_workforce_by_skill'].sum() / len(
                        day_group['unavailable_workforce_by_skill'])),
                    'available_workforce_per_by_skill': (
                        day_group['available_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        day_group['available_workforce_per_by_skill'].mean(skipna=True)) else None,
                    'unavailable_workforce_per_by_skill': (
                        day_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        day_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) else None,

                }
               # print(datamart_dict_individual)
                workforce_dict_day_time[department_id][day] = datamart_dict_individual

        individual_datamart_day_time = {}
        params_delete = {"timestamp": midnight_ts}
        wdm_workforce_availability_daily_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wdm_workforce_availability_daily(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }


            """)
        await send_mutation(wdm_workforce_availability_daily_table, params_delete)

        for department_id, wdm_dict_day_time_row in workforce_dict_day_time.items():
            variable_name = f"wdm_{department_id}"
            individual_datamart_day_time[variable_name] = wdm_dict_day_time_row

            formatted_data = [item[1] for item in wdm_dict_day_time_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            mutation_objects = formatted_data
            wdm_workforce_availability_daily_table = gql(
                """
            mutation MyMutation($objects: [wdm_workforce_availability_daily_insert_input!] = {}) {
                  insert_wdm_workforce_availability_daily(objects: $objects) {
                    affected_rows
                  }
                }

                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wdm_workforce_availability_daily_table, params)

        return "Success!"

    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/wdm_workforce_availability_weekly")
async def wdm_workforce_availability_weekly():
    try:
        variables = {"timestamp": getBefore_n_DaysEpochTimestamp(7)}
        print(getBefore_n_DaysEpochTimestamp(7))

        #variables = {"timestamp": 1704689530000}
        df = pd.DataFrame(await getWdmWorforceAvailability(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")

        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}

        workforce_dict_week_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_dict_week_time[department_id] = {}

            for week, week_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('W')):
                datamart_dict_individual = {
                    'start_date': week.start_time.strftime("%Y-%m-%d"),
                    'end_date': week.end_time.strftime("%Y-%m-%d"),

                    'timestamp': week_group['timestamp'].iloc[-1],
                    'tenantid': week_group['tenantid'].iloc[-1],
                    'org_id': week_group['org_id'].iloc[-1],
                    'unit_id': week_group['unit_id'].iloc[-1],
                    'department_id': week_group['department_id'].iloc[-1],
                    'shift': week_group['shift'].iloc[-1],
                    'skill': week_group['skill'].iloc[-1],
                    'job_role': week_group['job_role'].iloc[-1],
                    'total_workforce': math.ceil(week_group['total_workforce'].sum() / len(week_group['total_workforce'])),
                    'available_workforce': math.ceil(
                        week_group['available_workforce'].sum() / len(week_group['available_workforce'])),
                    'unavailable_workforce': math.ceil(
                        week_group['unavailable_workforce'].sum() / len(week_group['unavailable_workforce'])),
                    'workforce_availability_per': (
                        week_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(
                        week_group['workforce_availability_per'].mean(skipna=True)) else None,
                    'unavailable_workforce_per': (
                        week_group['unavailable_workforce_per'].mean(skipna=True)) if not np.isnan(
                        week_group['unavailable_workforce_per'].mean(skipna=True)) else None,
                    'available_workforce_by_job_role': math.ceil(week_group['available_workforce_by_job_role'].sum() / len(
                        week_group['available_workforce_by_job_role'])),
                    'available_workforce_per_by_job_role': (
                        week_group['available_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        week_group['available_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'unavailable_workorce_by_job_role': math.ceil(
                        week_group['unavailable_workorce_by_job_role'].sum() / len(
                            week_group['unavailable_workorce_by_job_role'])),
                    'unavailable_workforce_per_by_job_role': (
                        week_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        week_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'available_workforce_by_skill': math.ceil(
                        week_group['available_workforce_by_skill'].sum() / len(week_group['available_workforce_by_skill'])),
                    'unavailable_workforce_by_skill': math.ceil(week_group['unavailable_workforce_by_skill'].sum() / len(
                        week_group['unavailable_workforce_by_skill'])),
                    'available_workforce_per_by_skill': (
                        week_group['available_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        week_group['available_workforce_per_by_skill'].mean(skipna=True)) else None,
                    'unavailable_workforce_per_by_skill': (
                        week_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        week_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) else None,

                }
                workforce_dict_week_time[department_id][week] = datamart_dict_individual

        # print(datamart_dict_day_time)
        individual_datamart_day_time = {}
        params_delete = {"timestamp": getBefore_n_DaysEpochTimestamp(7)}
        wdm_workforce_availability_weekly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wdm_workforce_availability_weekly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wdm_workforce_availability_weekly_table, params_delete)

        for department_id, workforce_dict_week_time_row in workforce_dict_week_time.items():
            variable_name = f"wdm_{department_id}"
            individual_datamart_day_time[variable_name] = workforce_dict_week_time_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_week_time_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            # print(formatted_data)

            mutation_objects = formatted_data
            wdm_workforce_availability_weekly_table = gql(
                """
                mutation MyMutation($objects: [wdm_workforce_availability_weekly_insert_input!] = {}) {
                      insert_wdm_workforce_availability_weekly(objects: $objects) {
                        affected_rows
                      }
                    }

                    """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wdm_workforce_availability_weekly_table, params)

        return {
            "status": "SUCCESS",
        }

    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }


@app.post("/wdm_workforce_availability_monthly")
async def wdm_workforce_availability_monthly():
    try:
        timestamp = get_start_epoch_time_of_yesterdays_month()
        print(timestamp)
        variables = {"timestamp": timestamp}
        #variables = {"timestamp": 1704689530000}
        df = pd.DataFrame(await getWdmWorforceAvailability(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}

        workforce_dict_month_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_dict_month_time[department_id] = {}

            for month, month_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('M')):
                datamart_dict_individual = {
                    'start_date': month.start_time.strftime("%Y-%m-%d"),
                    'end_date': month.end_time.strftime("%Y-%m-%d"),

                    'timestamp': month_group['timestamp'].iloc[-1],
                    'tenantid': month_group['tenantid'].iloc[-1],
                    'org_id': month_group['org_id'].iloc[-1],
                    'unit_id': month_group['unit_id'].iloc[-1],
                    'department_id': month_group['department_id'].iloc[-1],
                    'shift': month_group['shift'].iloc[-1],
                    'skill': month_group['skill'].iloc[-1],
                    'job_role': month_group['job_role'].iloc[-1],
                    'total_workforce': math.ceil(
                        month_group['total_workforce'].sum() / len(month_group['total_workforce'])),
                    'available_workforce': math.ceil(
                        month_group['available_workforce'].sum() / len(month_group['available_workforce'])),
                    'unavailable_workforce': math.ceil(
                        month_group['unavailable_workforce'].sum() / len(month_group['unavailable_workforce'])),
                    'workforce_availability_per': (
                        month_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(
                        month_group['workforce_availability_per'].mean(skipna=True)) else None,
                    'unavailable_workforce_per': (
                        month_group['unavailable_workforce_per'].mean(skipna=True)) if not np.isnan(
                        month_group['unavailable_workforce_per'].mean(skipna=True)) else None,
                    'available_workforce_by_job_role': math.ceil(
                        month_group['available_workforce_by_job_role'].sum() / len(
                            month_group['available_workforce_by_job_role'])),
                    'available_workforce_per_by_job_role': (
                        month_group['available_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        month_group['available_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'unavailable_workorce_by_job_role': math.ceil(
                        month_group['unavailable_workorce_by_job_role'].sum() / len(
                            month_group['unavailable_workorce_by_job_role'])),
                    'unavailable_workforce_per_by_job_role': (
                        month_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        month_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'available_workforce_by_skill': math.ceil(
                        month_group['available_workforce_by_skill'].sum() / len(
                            month_group['available_workforce_by_skill'])),
                    'unavailable_workforce_by_skill': math.ceil(
                        month_group['unavailable_workforce_by_skill'].sum() / len(
                            month_group['unavailable_workforce_by_skill'])),
                    'available_workforce_per_by_skill': (
                        month_group['available_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        month_group['available_workforce_per_by_skill'].mean(skipna=True)) else None,
                    'unavailable_workforce_per_by_skill': (
                        month_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        month_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) else None,

                }
                workforce_dict_month_time[department_id][month] = datamart_dict_individual

        # print(datamart_dict_day_time)
        individual_datamart_day_time = {}
        params_delete = {"timestamp": timestamp}
        wdm_workforce_availability_monthly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wdm_workforce_availability_monthly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }


            """)
        await send_mutation(wdm_workforce_availability_monthly_table, params_delete)

        for department_id, wdm_dict_month_time_row in workforce_dict_month_time.items():
            variable_name = f"wdm_{department_id}"
            individual_datamart_day_time[variable_name] = wdm_dict_month_time_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in wdm_dict_month_time_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            # print(formatted_data)

            mutation_objects = formatted_data
            wdm_workforce_availability_monthly_table = gql(
                """
                mutation MyMutation($objects: [wdm_workforce_availability_monthly_insert_input!] = {}) {
                      insert_wdm_workforce_availability_monthly(objects: $objects) {
                        affected_rows
                      }
                    }

                    """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wdm_workforce_availability_monthly_table, params)

        return {
            "status": "SUCCESS",
        }

    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }



@app.post("/wdm_workforce_availability_yearly")
async def wdm_workforce_availability_yearly():
    try:
        variables = {"timestamp": getBefore_n_MonthsEpochTimestamp(12)}
        print(getBefore_n_MonthsEpochTimestamp(12))
        #variables = {"timestamp": 1704689530000}
        df = pd.DataFrame(await getWdmWorforceAvailability(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in
                        df.groupby('department_id')}

        workforce_dict_year_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_dict_year_time[department_id] = {}

            for year, year_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('Y')):
                datamart_dict_individual = {
                    'start_date': year.start_time.strftime("%Y-%m-%d"),
                    'end_date': year.end_time.strftime("%Y-%m-%d"),

                    'timestamp': year_group['timestamp'].iloc[-1],
                    'tenantid': year_group['tenantid'].iloc[-1],
                    'org_id': year_group['org_id'].iloc[-1],
                    'unit_id': year_group['unit_id'].iloc[-1],
                    'department_id': year_group['department_id'].iloc[-1],
                    'shift': year_group['shift'].iloc[-1],
                    'skill': year_group['skill'].iloc[-1],
                    'job_role': year_group['job_role'].iloc[-1],
                    'total_workforce': math.ceil(year_group['total_workforce'].sum() / len(year_group['total_workforce'])),
                    'available_workforce': math.ceil(year_group['available_workforce'].sum() / len(year_group['available_workforce'])),
                    'unavailable_workforce': math.ceil(year_group['unavailable_workforce'].sum() / len(year_group['unavailable_workforce'])),
                    'workforce_availability_per': (year_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(year_group['workforce_availability_per'].mean(skipna=True)) else None,
                    'unavailable_workforce_per': (year_group['unavailable_workforce_per'].mean(skipna=True)) if not np.isnan(year_group['unavailable_workforce_per'].mean(skipna=True)) else None,
                    'available_workforce_by_job_role': math.ceil(year_group['available_workforce_by_job_role'].sum() / len(year_group['available_workforce_by_job_role'])),
                    'available_workforce_per_by_job_role': (year_group['available_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(year_group['available_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'unavailable_workorce_by_job_role': math.ceil(
                        year_group['unavailable_workorce_by_job_role'].sum() / len(
                            year_group['unavailable_workorce_by_job_role'])),
                    'unavailable_workforce_per_by_job_role': (
                        year_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) if not np.isnan(
                        year_group['unavailable_workforce_per_by_job_role'].mean(skipna=True)) else None,
                    'available_workforce_by_skill': math.ceil(
                        year_group['available_workforce_by_skill'].sum() / len(
                            year_group['available_workforce_by_skill'])),
                    'unavailable_workforce_by_skill': math.ceil(
                        year_group['unavailable_workforce_by_skill'].sum() / len(
                            year_group['unavailable_workforce_by_skill'])),
                    'available_workforce_per_by_skill': (
                        year_group['available_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        year_group['available_workforce_per_by_skill'].mean(skipna=True)) else None,
                    'unavailable_workforce_per_by_skill': (
                        year_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) if not np.isnan(
                        year_group['unavailable_workforce_per_by_skill'].mean(skipna=True)) else None,
                }
                workforce_dict_year_time[department_id][year] = datamart_dict_individual
        # print(datamart_dict_day_time)
        individual_datamart_year_time = {}
        params_delete = {"timestamp": 1704689530000}
        wdm_workforce_availability_yearly_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wdm_workforce_availability_yearly(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wdm_workforce_availability_yearly_table, params_delete)
        for department_id, wdm_dict_year_time_row in workforce_dict_year_time.items():
            variable_name = f"wdm_{department_id}"
            individual_datamart_year_time[variable_name] = wdm_dict_year_time_row
          # Integrate the two lines here
            formatted_data = [item[1] for item in wdm_dict_year_time_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            wdm_workforce_availability_yearly_table = gql(
                """
                mutation MyMutation($objects: [wdm_workforce_availability_yearly_insert_input!] = {}) {
                      insert_wdm_workforce_availability_yearly(objects: $objects) {
                        affected_rows
                      }
                    }

                    """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wdm_workforce_availability_yearly_table, params)

        return {
            "status": "SUCCESS",
        }

    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }


@app.post("/operator_machine_availability")
async def operatorMachineAvailability():
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
        variables = {"timestamp": midnight_ts}
        print(variables)
       # variables = {"ts": 1704707674000}
        operator_tag_dict = await getEmpTagAssignData()
        shift_dict = await getshiftStartshiftEnd()
        df = pd.DataFrame(await getProximityData(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms")
        shift_id_df = pd.DataFrame(await getShiftDict())
        shift_id_dict = create_shift_dict(shift_id_df)
        df["shift_id"] = AssignShiftId(df, shift_id_dict)
        # shiftStart time and shiftend time assign
        df = addShiftStartAndShiftEnd(df, shift_dict)
        # To assign org_id,unit_id,department_id
        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)

        separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in
                        df.groupby(df['timestamp'].dt.date)}

        # Dictionary to store machine actual production times
        operator_machine_availability_dict = {}
        out_param = 'out'
        # Iterate over machines
        for date, df_date in separate_dfs.items():
            operator_machine_availability_dict[date] = {}

            for date, df_date in separate_dfs.items():
                operator_machine_availability_dict[date] = {}

                # Iterate over hours for the current machine
                for (machine_id, shift_id, tenant_id, tag_id), shift_group in df_date.groupby(
                        [df_date['machineid'], df_date['shift_id'], df_date['tenantid'], df_date['tagid']]):
                    if not tag_id.startswith("die"):
                        in_param = shift_group['edgeid'].iloc[-1]
                        operator_id = assignOperatorToTag(df['tenantid'].iloc[0], tag_id, operator_tag_dict)
                        proximity_in_time_total = getProximityInTimeTotal(shift_group, out_param, in_param)
                        proximity_out_time_total = getProximityOutTimeTotal(shift_group, out_param, in_param)
                        print(proximity_in_time_total,proximity_out_time_total)
                        effective_proximity_time = calculateEffectiveProximityTime(proximity_in_time_total,
                                                                                   shift_group['shift_start'].iloc[-1],
                                                                                   shift_group['shift_end'].iloc[-1])
                        proximity_time_loss_per = calculateProximityLossTimePercentage(proximity_out_time_total,
                                                                                       shift_group['shift_start'].iloc[-1],
                                                                                       shift_group['shift_end'].iloc[-1])

                        datamart_dict_individual = {
                            'date': date,
                            'timestamp': shift_group['ts'].iloc[-1],
                            'tenantid': shift_group['tenantid'].iloc[-1],
                            'org_id': shift_group['org_id'].iloc[-1],
                            'unit_id': shift_group['unit_id'].iloc[-1],
                            'department_id': shift_group['department_id'].iloc[-1],
                            'machineid': shift_group['machineid'].iloc[-1],
                            'edgeid': in_param,

                            'tagid': tag_id,
                            'operator_id': operator_id,
                            'proximity_in_time_total': round(proximity_in_time_total),
                            'proximity_out_time_total': round(proximity_out_time_total),
                            'total_effective_proximity_time': effective_proximity_time,
                            'machine_proximity_time_loss_percent': proximity_time_loss_per,
                            'shift': shift_group['shift_id'].iloc[-1],

                        }

                        operator_machine_availability_dict[date][shift_id] = datamart_dict_individual
            individual_operator_machine_availability_dict = {}
            params_delete = {"timestamp": midnight_ts}
            wfl0_operator_machine_availability_table = gql(
                """
                 mutation MyMutation($timestamp: bigint = "") {
                          delete_wfl0_operator_machine_availability(where: {timestamp: {_gte: $timestamp}}) {
                            affected_rows
                          }
                        }
                """)
            await send_mutation(wfl0_operator_machine_availability_table, params_delete)

            for date, operator_machine_availability_dict in operator_machine_availability_dict.items():
                variable_name = f"fl0_machine_actual_production_time_{date}"
                individual_operator_machine_availability_dict[variable_name] = operator_machine_availability_dict

                # Integrate the two lines here
                formatted_data = [item[1] for item in operator_machine_availability_dict.items()]
                formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
                # print(formatted_data)
                mutation_objects = formatted_data
                dm_operator_machine_availability_table = gql(
                    """
                    mutation MyMutation($objects: [wfl0_operator_machine_availability_insert_input!] = {}) {
                      insert_wfl0_operator_machine_availability(objects: $objects) {
                        affected_rows
                      }
                    }
                    """
                )
                params = {"objects": mutation_objects}
                await send_mutation(dm_operator_machine_availability_table, params)

            return {
                "status": "SUCCESS",

            }
    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }


@app.post("/org_operators_machine_availability_shiftwise")
async def orgOperatorsMachineAvailability_shiftwise():
    try:
        variables = {"timestamp": midnight_ts}
        print(variables)
        #variables = {"timestamp": 1704689530000}
        df = pd.DataFrame(await getOperatorMachineAvailability(variables))
        df['timestamp_1'] = pd.to_datetime(df['timestamp'], unit="ms")
        if df.empty:
            return {"status": "Required Data is not available"}
        # print(df)
        separate_dfs = {name: group.sort_values(by='timestamp_1', ascending=True) for name, group in df.groupby('department_id')}
        operator_machine_availaibility_dict = {}

        for department_id, df_machine in separate_dfs.items():
            operator_machine_availaibility_dict[department_id] = {}

            for (day, shift), day_group in df_machine.groupby([df_machine['timestamp_1'].dt.to_period('D'), 'shift']):
                datamart_dict_individual = {
                    'date': day_group['date'].iloc[-1],
                    'timestamp': day_group['timestamp'].iloc[-1],
                    'tenantid': day_group['tenantid'].iloc[-1],
                    'org_id': day_group['org_id'].iloc[-1],
                    'unit_id': day_group['unit_id'].iloc[-1],
                    'department_id': day_group['department_id'].iloc[-1],
                    'machineid': day_group['machineid'].iloc[-1],
                    'edgeid': day_group['edgeid'].iloc[-1],
                    'shift': shift,
                    'tagid': day_group['tagid'].iloc[-1],
                    'operator_id': day_group['operator_id'].iloc[-1],
                    'proximity_in_time_total': math.ceil(day_group['proximity_in_time_total'].sum()),
                    'proximity_out_time_total': math.ceil(day_group['proximity_out_time_total'].sum()),
                    'total_effective_proximity_time': (
                        day_group['total_effective_proximity_time'].mean(skipna=True)) if not np.isnan(
                        day_group['total_effective_proximity_time'].mean(skipna=True)) else None,
                    'machine_proximity_time_loss_percent': (
                        day_group['machine_proximity_time_loss_percent'].mean(skipna=True)) if not np.isnan(
                        day_group['machine_proximity_time_loss_percent'].mean(skipna=True)) else None

                }
                operator_machine_availaibility_dict[department_id][day] = datamart_dict_individual
        individual_operator_machine_availaibility_dict = {}
        params_delete = {"timestamp": midnight_ts}
        wfl0_operators_machine_availability_shiftwise_table = gql(
            """
             mutation MyMutation($timestamp: bigint = "") {
                      delete_wfl0_operators_machine_availability_shiftwise(where: {timestamp: {_gte: $timestamp}}) {
                        affected_rows
                      }
                    }
            """)
        await send_mutation(wfl0_operators_machine_availability_shiftwise_table, params_delete)

        for department_id, operator_machine_availaibility_dict_row in operator_machine_availaibility_dict.items():
            variable_name = f"wdm_{department_id}"
            individual_operator_machine_availaibility_dict[variable_name] = operator_machine_availaibility_dict_row
            # Integrate the two lines here
            formatted_data = [item[1] for item in operator_machine_availaibility_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            wdm_workforce_availability_shiftwise_table = gql(
                """
                 mutation MyMutation($objects: [wfl0_operators_machine_availability_shiftwise_insert_input!] = {}) {
                  insert_wfl0_operators_machine_availability_shiftwise(objects: $objects) {
                    affected_rows
                  }
                }
               """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wdm_workforce_availability_shiftwise_table, params)
            return {
                "status": "SUCCESS",
            }

    except Exception as e:
        return {
            "status": "ERROR",
            "message": str(e)
        }


if __name__ == '__main__':
    uvicorn.run(app, port=8082, host='0.0.0.0')
