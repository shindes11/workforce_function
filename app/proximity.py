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
    checkTagIdPresence, getProximityInTimeTotal, getProximityOutTimeTotal, getOperatorMachineInTimeTotal, createDictTenantOperator, getOperatorName,\
    create_shift_dict, AssignShiftId, getFifteenMinOpertorTime, addShiftStartAndShiftEnd, calculateEffectiveProximityTime,\
    calculateProximityLossTimePercentage, absenteeismRatioMonthly, absenteeismRatioWeekly,getBefore_n_HoursEpochTimestamp,\
    getTotalWorkforce, getOperatorJobRole,getOperatorSalary, getOperatorGender, getOperatorCertification, getOperatorTotalExperience,\
    getOperatorEducation, getOperatorMaritalStatus, workforceAvailabilityPercentage, getEndCustomer, getPartId ,fetchProximityInTime ,\
    fetchPlannedProductionTime,calculateEfficiencyTime,fetchActualProductionTime ,fetchTargetPart,fetchTotalPartsProduced,calculateEfficiencyToProducedParts ,\
fetchQualityPercentage,getOle,assign_org_unit_dept ,getOperatorSkills,fetchRunTime,calculateWorkforceUtilization ,getTotalavailableWorkforce,AvailableWorkforceByJobRole,\
AvailableWorkforceBySkill

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

async def getoperatorAbsenteeismdf():
    query1 = gql(
        """
        query MyQuery {
          wfl0_operator_absenteeism {
            date
            tenantid
            operator_id
            operator_name
            is_absent
          }
        }


        """)
    data1 = await get_data1(query1)
    return data1['wfl0_operator_absenteeism']

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
async def getEmpOperatorName():
    query1 = gql(
        """
        query MyQuery {
          tenant_employees(where: {tenantid: {_is_null: false}}) {
            employee_id
            first_name
            last_name
            tenantid
          }
        }


        """)
    data = await get_data1(query1)
    return data['tenant_employees']

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

async def getOrgUnitDeptDict():
    query2 = gql("""
        query MyQuery {
          tenant_employees {
            employee_id
            tenantid
            organization_id
            plant
            department
          }
        }

        """)
    data = await get_data1(query2)
    return data['tenant_employees']

async def getTenantplanDict():
    query2 = gql("""
        query MyQuery {
          tnt_production_plan_master {
            date
            shiftname
            tenantid
            planid
          }
        }

        """)
    data = await get_data1(query2)
    return data['tnt_production_plan_master']
async def getPlanOperatorDict():
    query2 = gql("""
           query MyQuery {
              operator_production_plan_assignment {
                tenantid
                plan_id
                operator_id
                date
              }
            }
            """)
    data = await get_data1(query2)
    return data['operator_production_plan_assignment']

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
    query = gql ("""
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



# To get tnt_work_schedule_masters dict to assign shift_start and shift_end time to df
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
# To get oa_batch_details dict
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
# To get dm_shiftwise df
async def getDMShiftDf(variable):
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
# To gat dm_operator_machine_availability df
async def getOperatorMachineAvailabilityDf(variable):
    query = gql("""
        query MyQuery($ts: bigint = "") {
              dm_operator_machine_availability(where: {timestamp: {_gte: $ts}}) {
                date
                machineid
                marital_status
                operator_id
                org_id
                proximity_in_time
                proximity_out_time
                salary
                shift
                skill
                tagid
                tenantid
                timestamp
                total_effective_proximity_time
                unit_id
                job_role
                machine_proximity_time_loss_per
                gender
                experience
                education
                edgeid
                department_id
              }
            }
   
    """)
    data = await get_data(query,variable)
    return data['dm_operator_machine_availability']
# to get tnt_org_machine_assignment df
async def getOrgUnitDeptdf(variable):
    query = gql("""
        
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
    """)
    data = await get_data(query,variable)
    return data['tnt_org_machine_assignment']
# to get workforce availability dataframe
async def getWfl0WorkforceAvailability(variable):
    query2 = gql("""
    query MyQuery( $timestamp: bigint = "") {
          wfl0_workforce_availability(where: {timestamp: {_gte: $timestamp}}) {
            available_workforce
            date
            department_id
            org_id
            planned_workforce
            shift
            tenantid
            timestamp
            unit_id
            workforce_availability_per
          }
        }
    """)
    data = await get_data(query2,variable)
    return data['wfl0_workforce_availability']
# get the workforce_effieciency df
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






@app.post("/Operator Proximity Log")
async def operatorProximityLog():
    try:
        variables = {"ts": getBefore_n_HoursEpochTimestamp}

        df = pd.DataFrame(await getProximityData(variables))
        #df = pd.read_csv("/Users/HALCYON007/proximity_feb9tofeb10.csv")
        #df = pd.read_csv("/Users/HALCYON007/proximity_feb7to9.csv")
    #    df = pd.read_csv("/Users/HALCYON007/proximity_feb6.csv")
       # df = pd.read_csv("/Users/HALCYON007/proximity_jan20tojan24.csv")
        #df = pd.read_csv("/Users/HALCYON007/proximity_jan25to26.csv")
        #df = pd.read_csv("/Users/HALCYON007/proximity_feb10tofeb13.csv")

        if df.empty:
            return {"status": "Required Data is not available"}

        print(df)
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

            operator_proximity_dict.append(operator_proximity_log_dict)

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




# operator machine time
@app.post("/wfl0_operator_machine_time")
async def wfl0OperatorMachineTime():
    try:
        variables = {"ts": 1704699593000}
        df = pd.DataFrame(await getProximityData(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms", origin='unix')
        out_param = 'out'
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms")
        operator_tag_dict = await getEmpTagAssignData()

        # Group by machineid
        separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in df.groupby('machineid')}

        # Dictionary to store machine actual production times
        wfl0_operator_machine_time_dict = {}

        # Iterate over machines
        for machine_id, df_machine in separate_dfs.items():
            wfl0_operator_machine_time_dict[machine_id] = {}

            # Iterate over hours for the current machine
            for (date, hour, tag_id), hour_group in df_machine.groupby([df_machine['timestamp'].dt.date, df_machine['timestamp'].dt.hour, df_machine['tagid']]):
                in_param = hour_group['edgeid'].iloc[-1]
                operator_id = assignOperatorToTag(df['tenantid'].iloc[0], tag_id, operator_tag_dict)
                machine_in_time = getOperatorMachineInTimeTotal(hour_group, out_param, in_param)

                if machine_in_time > 0:
                    wfl0_operator_machine_time_individual = {
                        'date': date,
                        'timestamp': hour_group['ts'].iloc[-1],
                        'tenant_id': hour_group['tenantid'].iloc[-1],
                        'machine_id': hour_group['machineid'].iloc[-1],
                        'edge_id': in_param,
                        'tag_id': tag_id,
                        'operator_id': operator_id,
                        'timespent_on_machine': machine_in_time
                    }

                    wfl0_operator_machine_time_dict[machine_id][hour] = wfl0_operator_machine_time_individual

        # Push individual machine actual production times to the database
        individual_wfl0_operator_machine_time = {}
        for machine_id, operator_machine_time_dict in wfl0_operator_machine_time_dict.items():
            variable_name = f"operator_machine_time_{machine_id}"
            individual_wfl0_operator_machine_time[variable_name] = operator_machine_time_dict
            # Integrate the two lines here
            formatted_data = [item[1] for item in operator_machine_time_dict.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            wfl0_operator_machine_time_table = gql(
                """
                mutation MyMutation($objects: [wfl0_operator_proximity_machine_time_insert_input!] = {}) {
                    insert_wfl0_operator_proximity_machine_time(objects: $objects) {
                    affected_rows
                     }
                 }
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_operator_machine_time_table, params)

        return {
            "status": "SUCCESS",

        }
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }

#operator machine analysis
@app.post("/wfl0_operator_machine_analysis")
async def OperatorMachineAnalysis():
    try:
        variables = {"ts": 1704707674000}
        df = pd.DataFrame(await getProximityData(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms", origin='unix')

        # hard coded
        out_param = 'out'
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms")

        # Group by machineid
        separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in df.groupby('machineid')}

        # Dictionary to store machine actual production times
        wfl0_operator_machine_analysis_dict = {}

        # Iterate over machines
        for machine_id, df_machine in separate_dfs.items():
            wfl0_operator_machine_analysis_dict[machine_id] = {}

            # Iterate over hours for the current machine
            for (date, hour,fifteen_min, tag_id), fifteen_min_group in df_machine.groupby([df_machine['timestamp'].dt.date, df_machine['timestamp'].dt.hour, df_machine['timestamp'].dt.floor('15min'),df_machine['tagid']]):
                in_param = fifteen_min_group['edgeid'].iloc[-1]
                machine_in_time = getFifteenMinOpertorTime(fifteen_min_group, out_param, in_param)
               # print('date',date,'hour',hour,'fittenmin',fifteen_min, 'machine_id',machine_id,'tag_id',tag_id,'time',machine_in_time)
                if machine_in_time > 0:
                    wfl0_operator_machine_analysis_dict_individual = {
                        'date': date,
                        'timestamp': fifteen_min_group['ts'].iloc[-1],
                        'tenant_id': fifteen_min_group['tenantid'].iloc[-1],
                        'machine_id': fifteen_min_group['machineid'].iloc[-1],
                        'edge_id': in_param,
                        'tag_id': tag_id,
                        'timespent_on_machine': round(machine_in_time)
                    }

                    wfl0_operator_machine_analysis_dict[machine_id][fifteen_min] = wfl0_operator_machine_analysis_dict_individual

        # Push individual machine actual production times to the database
        individual_wfl0_operator_machine_analysis_dict = {}
        for machine_id, operator_machine_analysis_dict in wfl0_operator_machine_analysis_dict.items():
            variable_name = f"operator_machine_analysis_{machine_id}"
            individual_wfl0_operator_machine_analysis_dict[variable_name] = operator_machine_analysis_dict

            # Integrate the two lines here
            formatted_data = [item[1] for item in operator_machine_analysis_dict.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            mutation_objects = formatted_data
            wfl0_operator_machine_proximity_analysis_table = gql(
                """
                mutation MyMutation($objects: [wfl0_operator_machine_proximity_analysis_insert_input!] = {}) {
                  insert_wfl0_operator_machine_proximity_analysis(objects: $objects) {
                    affected_rows
                  }
                }
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_operator_machine_proximity_analysis_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }




@app.post("/operator_absenteeism")
async def operatorAbsenteeism():
    try:
        variables = {"ts": 1704707674000}
        #operator_name Dict to assign name to the operator
        operator_Name_Dict = await getEmpOperatorName()
        operator_tag_dict = await getEmpTagAssignData()
        tenant_tags = pd.DataFrame(await getEmpTagAssignData())
        unique_operator_ids_dict = createDictTenantOperator(tenant_tags)
        df = pd.DataFrame(await getProximityData(variables))
        if df.empty:
            return {"status": "Required Data is not available"}

        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms")
        # Group by machineid
        separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in df.groupby(df['timestamp'].dt.date)}

        # Dictionary to store machine actual production times
        operatorAbsenteeism_dict = {}

        # Iterate over machines
        for date, df_date in separate_dfs.items():
            operatorAbsenteeism_dict[date] = {}
            count = 0
            # Iterate over hours for the current machine
            for (tenant_id, tag_id), date_group in df_date.groupby([df_date['tenantid'], df_date['tagid']]):
                operator_present_absent = checkTagIdPresence(date_group, date, unique_operator_ids_dict)
                operator_id = assignOperatorToTag(date_group['tenantid'].iloc[-1], tag_id, operator_tag_dict)
                operator_name = getOperatorName(operator_Name_Dict, operator_id, tenant_id)
               # print('date ',date, 'tenant_id',tenant_id, "operator_id", operator_id,'operator_name',operator_name,'operator_present_absent' , operator_present_absent)
                operatorAbsenteeism_dict = {
                    'date':  date_group['timestamp'].iloc[-1].strftime("%Y-%m-%d"),
                    'tenantid': date_group['tenantid'].iloc[-1],
                    'operator_id': operator_id,
                    'operator_name': operator_name,
                    'is_absent': operator_present_absent
                }

                operatorAbsenteeism_dict[date][count] = operatorAbsenteeism_dict
              #  print( ' machine_actual_production_time_dict', machine_actual_production_time_dict)
                count += 1
        for date, operatorAbsenteeism_dict in operatorAbsenteeism_dict.items():
            formatted_data = [item[1] for item in operatorAbsenteeism_dict.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            #print(formatted_data)
            mutation_objects = formatted_data
            wfl0_operator_absenteeism_table = gql(
                    """
                    mutation MyMutation($objects: [wfl0_operator_absenteeism_insert_input!] = {}) {
                          insert_wfl0_operator_absenteeism(objects: $objects) {
                            affected_rows
                          }
                          }
                    """
                )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_operator_absenteeism_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


# operator_absenteeism Ratio weekly

@app.post("/operator_absenteeism_ratio_weekly")
async def operatorAbesenteeism():
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
        df = pd.DataFrame(await getoperatorAbsenteeismdf())
        if df.empty:
            return {"status": "Required Data is not available"}
        employee_dict = await getOrgUnitDeptDict()
        df['date'] = pd.to_datetime(df['date'])

        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)

        separate_dfs = df.groupby([pd.Grouper(key='date', freq='W'), 'tenantid', 'operator_id']).apply(lambda group: group.reset_index(drop=True))

        operatorAbesenteeism_list = []  # Accumulate records in a list

        for (week, tenant_id, operator_id), df_group in separate_dfs.groupby(level=[0, 1, 2]):
            absenteeism_ratio = absenteeismRatioWeekly(df_group, tenant_id, operator_id)

            operatorAbsenteeism_dict_individual = {
                'start_date': df_group['date'].iloc[0],
                'end_date': df_group['date'].iloc[-1],
                'tenantid': tenant_id,
                'operator_id': operator_id,
                'operator_name': df_group['operator_name'].iloc[-1],
                'absenteeism_ratio': absenteeism_ratio,
                'org_id': df_group['org_id'].iloc[-1],
                'unit_id': df_group['unit_id'].iloc[-1],
                'department_id': df_group['department_id'].iloc[-1]
            }

            operatorAbesenteeism_list.append(operatorAbsenteeism_dict_individual)  # Append each record

        # Integrate the two lines here
        formatted_data = json.loads(json.dumps(operatorAbesenteeism_list, default=convert_to_json_serializable))
        #print(formatted_data
        mutation_objects = formatted_data
        wfl0_operator_absenteeism_ratio_weekly_table = gql(
            """
            mutation MyMutation($objects: [wfl0_operator_absenteeism_ratio_weekly_insert_input!] = {}) {
                  insert_wfl0_operator_absenteeism_ratio_weekly(objects: $objects) {
                    affected_rows
                  }
                }
            """
        )
        params = {"objects": mutation_objects}
        await send_mutation(wfl0_operator_absenteeism_ratio_weekly_table, params)

        return {
            "status": "SUCCESS",
        }
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


#operator_ absenteeism ratio monthswise
@app.post("/operator_absenteeism_ratio_month")
async def operatorAbesenteeism():
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

        df = pd.DataFrame(await getoperatorAbsenteeismdf())
        if df.empty:
            return {"status": "Required Data is not available"}
        employee_dict = await getOrgUnitDeptDict()
        df['date'] = pd.to_datetime(df['date'])

        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)

        # Grouping by months and then by tenant_id and operator_id
        separate_dfs = df.groupby([pd.Grouper(key='date', freq='ME'), 'tenantid', 'operator_id']).apply(lambda group: group.reset_index(drop=True))
        operatorAbesenteeism_list = []  # Accumulate records in a list
        for (month, tenant_id, operator_id), df_group in separate_dfs.groupby(level=[0, 1, 2]):
            absenteeism_ratio = absenteeismRatioMonthly(df_group, tenant_id, operator_id)
            operator_abesenteeism_dict_individual = {
                'date': df_group['date'].iloc[-1],
                'tenantid': tenant_id,
                'operator_id': operator_id,
                'operator_name': df_group['operator_name'].iloc[-1],
                'absenteeism_ratio': absenteeism_ratio,
                'org_id': df_group['org_id'].iloc[-1],
                'unit_id': df_group['unit_id'].iloc[-1],
                'department_id': df_group['department_id'].iloc[-1]

            }

            operatorAbesenteeism_list.append(operator_abesenteeism_dict_individual)  # Append each record
        formatted_data = json.loads(json.dumps(operatorAbesenteeism_list, default=convert_to_json_serializable))
        #print(formatted_data)
        mutation_objects = formatted_data

        wfl0_operator_absenteeism_ratio_table = gql(
            """
              mutation MyMutation($objects: [wfl0_operator_absenteeism_ratio_insert_input!] = {}) {
                  insert_wfl0_operator_absenteeism_ratio(objects: $objects) {
                    affected_rows
                  }
                }
    
            """
        )

        params = {"objects": mutation_objects}
        await send_mutation(wfl0_operator_absenteeism_ratio_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/wdm_operator Machine_availability")
async def wdmoperatorAvailability():
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

        variables = {"ts": 1704699593000}


        operator_tag_dict = await getEmpTagAssignData()

        shift_dict  = await getshiftStartshiftEnd()
        df = pd.DataFrame(await getProximityData(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms")
        shift_id_df = pd.DataFrame(await getShiftDict())
        shift_id_dict = create_shift_dict(shift_id_df)
        df["shift_id"] = AssignShiftId(df, shift_id_dict)
        df = addShiftStartAndShiftEnd(df ,shift_dict)
        employee_dict = await getOrgUnitDeptDict()
        tenant_emp_dict = await tenant_employee_dict()
        df2 = pd.DataFrame(await getoperatorSkillDict())
        job_role_dict = await getOpertorJobRoleDict()
        confige_code_dict = await getConfigeCodeDict()
        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)

        separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in df.groupby(df['timestamp'].dt.date)}

        # Dictionary to store machine actual production times
        dm_availability_dict = {}
        out_param= 'out'
        # Iterate over machines
        for date, df_machine in separate_dfs.items():
            dm_availability_dict[date] = {}

            # Iterate over hours for the current machine
            for (machine_id,shift_id,tenant_id, tag_id), hour_group in df_machine.groupby([df_machine['machineid'], df_machine['shift_id'], df_machine['tenantid'],df_machine['tagid']]):
                if not tag_id.startswith("die"):
                    in_param = hour_group['edgeid'].iloc[-1]
                    operator_id = assignOperatorToTag(df['tenantid'].iloc[0], tag_id, operator_tag_dict)
                    job_role = getOperatorJobRole(operator_id, tenant_emp_dict, job_role_dict)
                    salary = getOperatorSalary(tenant_emp_dict, operator_id, tenant_id)
                    gender = getOperatorGender(tenant_emp_dict, operator_id, tenant_id)
                    certification = getOperatorCertification(tenant_emp_dict, operator_id, tenant_id)
                    experience = getOperatorTotalExperience(tenant_emp_dict, operator_id, tenant_id)
                    Education = getOperatorEducation(tenant_emp_dict, confige_code_dict, operator_id, tenant_id)
                    marital_status = getOperatorMaritalStatus(tenant_emp_dict, confige_code_dict, operator_id, tenant_id)
                    proximity_in_time_total = getProximityInTimeTotal(hour_group,out_param,in_param)
                    proximity_out_time_total = getProximityOutTimeTotal(hour_group,out_param,in_param)
                    effective_proximity_time = calculateEffectiveProximityTime(proximity_in_time_total,hour_group['shift_start'].iloc[-1],hour_group['shift_end'].iloc[-1])
                    proximity_time_loss_per = calculateProximityLossTimePercentage(proximity_out_time_total,hour_group['shift_start'].iloc[-1],hour_group['shift_end'].iloc[-1])
                    skill = getOperatorSkills(df2, tenant_id, operator_id)
                    #print('operator_id',operator_id ,'job_role',job_role)

                    datamart_dict_individual = {
                        'date': date,
                        'timestamp': hour_group['ts'].iloc[-1],
                        'tenantid': hour_group['tenantid'].iloc[-1],
                        'machineid': hour_group['machineid'].iloc[-1],
                        'edgeid': in_param,
                        'org_id': hour_group['org_id'].iloc[-1],
                        'unit_id': hour_group['unit_id'].iloc[-1],
                        'department_id': hour_group['department_id'].iloc[-1],
                        'tagid': tag_id,
                        'operator_id': operator_id,
                        'proximity_in_time': round(proximity_in_time_total) if proximity_in_time_total is not None else None,
                        'proximity_out_time': round(proximity_out_time_total) if proximity_out_time_total is not None else None,
                        'total_effective_proximity_time': effective_proximity_time,
                        'machine_proximity_time_loss_per': proximity_time_loss_per,
                        'shift': hour_group['shift_id'].iloc[-1],
                        'job_role': job_role,
                        'salary': salary,
                        'gender': gender,
                        'skill': skill,
                        'certification': certification,
                        'experience': experience,
                        'education': Education,
                        'marital_status': marital_status
                    }

                    dm_availability_dict[date][shift_id] = datamart_dict_individual
        individual_dm_availability_dict = {}

        for date, dm_availability_dict in dm_availability_dict.items():
            variable_name = f"fl0_machine_actual_production_time_{date}"
            individual_dm_availability_dict[variable_name] = dm_availability_dict

            # Integrate the two lines here
            formatted_data = [item[1] for item in dm_availability_dict.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
           # print(formatted_data)
            mutation_objects = formatted_data
            dm_operator_machine_availability_table = gql(
                """
                mutation MyMutation($objects: [dm_operator_machine_availability_insert_input!] = {}) {
                  insert_dm_operator_machine_availability(objects: $objects) {
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
            "status": "FAILURE",
            "error_message": str(e)
        }



@app.post("/Shiftwise_workforce_availability")
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

        variables = {"ts": 1704707674000}

        df = pd.DataFrame(await getProximityData(variables))
        if df.empty:
            return {"status": "Required Data is not available"}
        df['timestamp'] = pd.to_datetime(df['ts'], unit="ms")
        shift_id_df = pd.DataFrame(await getShiftDict())
        shift_id_dict = create_shift_dict(shift_id_df)
        df["shift_id"] = AssignShiftId(df, shift_id_dict)
        assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
        df = assign_org_unit_dept(df, assignment_dict)
        operator_dict = await getActualWorkforcedict()

        # Group by machineid
        separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in  df.groupby(df['timestamp'].dt.date)}
        workforce_availability_dict = {}
        for date, df_date in separate_dfs.items():
            workforce_availability_dict[date] = {}
          # Iterate over hours for the current machine
            for (tenantid,shift_id), df_group in df_date.groupby([df_date['tenantid'],df_date['shift_id']]):
                planned_workforce = getTotalWorkforce(operator_dict, tenantid)
                availabe_workforce = getTotalavailableWorkforce(df_group)
                workforce_availability_per = workforceAvailabilityPercentage(planned_workforce, availabe_workforce)
               # print('date', date, 'tenantid', tenant_id, 'shift_id', df_group['shift_id'].iloc[-1], 'actual_workforce', planned_workforce, 'available_workforce', availabe_workforce ,'org_id',df_group['org_id'].iloc[-1])
                workforce_availability_dict = {
                    'date': date,
                    'timestamp': df_group['ts'].iloc[-1],
                    'tenantid': tenantid,
                    'org_id':df_group['org_id'].iloc[-1],
                    'unit_id':df_group['unit_id'].iloc[-1],
                    'department_id': df_group['department_id'].iloc[-1],
                    'shift': df_group['shift_id'].iloc[-1],
                    'planned_workforce': planned_workforce,
                    'available_workforce': availabe_workforce,
                    'workforce_availability_per': workforce_availability_per

                }

                workforce_availability_dict[date][shift_id] = workforce_availability_dict
        # Push individual machine actual production times to the database
        individual_workforce_availability_dict = {}
        for date, workforce_availability_dict in workforce_availability_dict.items():
            variable_name = f"workforce_availability_dict_{date}"
            individual_workforce_availability_dict[variable_name] = workforce_availability_dict

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_availability_dict.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            # print(formatted_data)
            mutation_objects = formatted_data
            wfl0_workforce_availability_shiftwise_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_availability_shiftwise_insert_input!] = {}) {
                              insert_wfl0_workforce_availability_shiftwise(objects: $objects) {
                                affected_rows
                              }
                            }
    
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wfl0_workforce_availability_shiftwise_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/daily_workforce_availability")
async def dailyWorkforceAvailability():
    try:
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0WorkforceAvailability(variable))
        if df.empty:
            return {"status": "Required Data is not available"}

        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        #print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}


        workforce_dict_day_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_dict_day_time[department_id] = {}

            for day, day_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('D')):
                avg = day_group['available_workforce'].sum() / len(day_group['planned_workforce'])
                workforce_dict = {
                    'date': day_group['date'].iloc[-1],
                    'timestamp': day_group['timestamp'].iloc[-1],
                    'tenantid': day_group['tenantid'].iloc[-1],
                    'org_id': day_group['org_id'].iloc[-1],
                    'unit_id': day_group['unit_id'].iloc[-1],
                    'department_id': department_id,
                    'shift': day_group['shift'].iloc[-1],
                    'planned_workforce': math.ceil(day_group['planned_workforce'].sum() / len(day_group['planned_workforce'])),
                    'available_workforce': math.ceil(day_group['available_workforce'].sum() / len(day_group['planned_workforce'])),
                    'workforce_availability_per': (day_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(day_group['workforce_availability_per'].mean(skipna=True)) else None
                }

                workforce_dict_day_time[department_id][day] = workforce_dict
        individual_workforce_dict_day_time = {}

        for department_id, workforce_dict_row in workforce_dict_day_time.items():
            variable_name = f"dm_foundation_{department_id}"
            individual_workforce_dict_day_time[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            # print(formatted_data)

            mutation_objects = formatted_data
            wflo_workforce_availability_daily_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_availability_daily_insert_input!] = {}) {
                  insert_wfl0_workforce_availability_daily(objects: $objects) {
                    affected_rows
                  }
                }
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_availability_daily_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/weekly_workforce_availability")
async def weeklyWorkforceAvailability():
    try:
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0WorkforceAvailability(variable))
        if df.empty:
            return {"status": "Required Data is not available"}

        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        #print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        workforce_dict_day_time = {}

        workforce_dict_weekly_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_dict_weekly_time[department_id] = {}

            for week, week_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('W')):
               # avg = day_group['available_workforce'].sum() / len(day_group['planned_workforce'])
                workforce_dict = {
                    'start_date': week.start_time.strftime("%Y-%m-%d"),
                    'end_date': week.end_time.strftime("%Y-%m-%d"),
                    'timestamp': week_group['timestamp'].iloc[-1],
                    'tenantid': week_group['tenantid'].iloc[-1],
                    'org_id': week_group['org_id'].iloc[-1],
                    'unit_id': week_group['unit_id'].iloc[-1],
                    'department_id': department_id,
                    'shift': week_group['shift'].iloc[-1],
                    'planned_workforce': math.ceil(week_group['planned_workforce'].sum() / len(week_group['planned_workforce'])),


                    'available_workforce': math.ceil(week_group['available_workforce'].sum() / len(week_group['planned_workforce'])),
                    'workforce_availability_per': (week_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(week_group['workforce_availability_per'].mean(skipna=True)) else None
                }

                workforce_dict_weekly_time[department_id][week] = workforce_dict
        individual_workforce_dict_weekly_time = {}

        for department_id, workforce_dict_row in workforce_dict_day_time.items():
            variable_name = f"dm_foundation_{department_id}"
            individual_workforce_dict_weekly_time[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))

            # print(formatted_data)

            mutation_objects = formatted_data
            wflo_workforce_availability_weekly_table = gql(
                """
               mutation
                    MyMutation($objects: [wfl0_workforce_availability_weekly_insert_input!] = {}) {
                        insert_wfl0_workforce_availability_weekly(objects: $objects) {
                        affected_rows
                    }
                    }
    
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_availability_weekly_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }


@app.post("/monthly_workforce_availability")
async def monthlyWorkforceAvailability():
    try:
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0WorkforceAvailability(variable))
        if df.empty:
            return {"status": "Required Data is not available"}

        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        #print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        workforce_dict_month_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_dict_month_time[department_id] = {}

            for month, month_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('M')):
               # avg = day_group['available_workforce'].sum() / len(day_group['planned_workforce'])
                workforce_dict = {
                    'date': month_group['date'].iloc[-1],
                    'timestamp': month_group['timestamp'].iloc[-1],
                    'tenantid': month_group['tenantid'].iloc[-1],
                    'org_id': month_group['org_id'].iloc[-1],
                    'unit_id': month_group['unit_id'].iloc[-1],
                    'department_id': department_id,
                    'shift': month_group['shift'].iloc[-1],
                    'planned_workforce': math.ceil(month_group['planned_workforce'].sum() / len(month_group['planned_workforce'])),
                    'available_workforce': math.ceil(month_group['available_workforce'].sum() / len(month_group['planned_workforce'])),
                    'workforce_availability_per': (month_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(month_group['workforce_availability_per'].mean(skipna=True)) else None
                }

                workforce_dict_month_time[department_id][month] = workforce_dict
        individual_workforce_dict_day_time = {}

        for department_id, workforce_dict_row in workforce_dict_month_time.items():
            variable_name = f"dm_foundation_{department_id}"
            individual_workforce_dict_day_time[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            wflo_workforce_availability_monthly_table = gql(
                """
                 mutation MyMutation($objects: [wfl0_workforce_availability_monthly_insert_input!] = {}) {
                      insert_wfl0_workforce_availability_monthly(objects: $objects) {
                        affected_rows
                      }
                    }
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_availability_monthly_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }

@app.post("/yearly_workforce_availability")
async def yearlyWorkforceAvailability():
    try:
        variable = {"timestamp": 1704689530000}

        df = pd.DataFrame(await getWfl0WorkforceAvailability(variable))
        if df.empty:
            return {"status": "Required Data is not available"}

        df['timestamp1'] = pd.to_datetime(df['timestamp'], unit="ms")
        #print(df)
        separate_dfs = {name: group.sort_values(by='timestamp', ascending=True) for name, group in df.groupby('department_id')}
        workforce_dict_yearly_time = {}

        for department_id, df_dept in separate_dfs.items():

            df_dept = df_dept.sort_values(by='timestamp', ascending=True)

            workforce_dict_yearly_time[department_id] = {}

            for month, month_group in df_dept.groupby(df_dept['timestamp1'].dt.to_period('Y')):
               # avg = day_group['available_workforce'].sum() / len(day_group['planned_workforce'])
                workforce_dict = {
                    'date': month_group['date'].iloc[-1],
                    'timestamp': month_group['timestamp'].iloc[-1],
                    'tenantid': month_group['tenantid'].iloc[-1],
                    'org_id': month_group['org_id'].iloc[-1],
                    'unit_id': month_group['unit_id'].iloc[-1],
                    'department_id': department_id,
                    'shift': month_group['shift'].iloc[-1],
                    'planned_workforce': math.ceil(month_group['planned_workforce'].sum() / len(month_group['planned_workforce'])),


                    'available_workforce': math.ceil(month_group['available_workforce'].sum() / len(month_group['planned_workforce'])),
                    'workforce_availability_per': (month_group['workforce_availability_per'].mean(skipna=True)) if not np.isnan(month_group['workforce_availability_per'].mean(skipna=True)) else None
                }

                workforce_dict_yearly_time[department_id][month] = workforce_dict
        individual_workforce_dict_yearly_time = {}

        for department_id, workforce_dict_row in workforce_dict_yearly_time.items():
            variable_name = f"dm_foundation_{department_id}"
            individual_workforce_dict_yearly_time[variable_name] = workforce_dict_row

            # Integrate the two lines here
            formatted_data = [item[1] for item in workforce_dict_row.items()]
            formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
            mutation_objects = formatted_data
            wflo_workforce_availability_yearly_table = gql(
                """
                mutation MyMutation($objects: [wfl0_workforce_availability_yearly_insert_input!] = {}) {
                      insert_wfl0_workforce_availability_yearly(objects: $objects) {
                        affected_rows
                      }
                    }
    
                """
            )
            params = {"objects": mutation_objects}
            await send_mutation(wflo_workforce_availability_yearly_table, params)
    except Exception as e:
        return {
            "status": "FAILURE",
            "error_message": str(e)
        }





# @app.post("/wdm_workfoce_utilization")
# async def dmworkforceUtilization():
#     try:
#         query2 = gql(
#             """
#             query MyQuery {
#                 tnt_org_machine_assignment {
#                     org_id
#                     unit
#                     department
#                     tenantid
#                     machineid
#                     edgeid
#                     machine_auto_id
#                 }
#             }
#             """
#         )
#         s2 = await get_data1(query2)  # tnt_org_machine_assignment to assign org_id,department_id, unit_id
#
#
#         variables = {"ts": 1704699593000}
#         df = pd.DataFrame(await getProximityData(variables))
#         if df.empty:
#             return {"status": "Required Data is not available"}
#         df['timestamp'] = pd.to_datetime(df['ts'], unit="ms")
#         shift_id_df = pd.DataFrame(await getShiftDict())
#         shift_id_dict = create_shift_dict(shift_id_df)
#         df["shift_id"] = AssignShiftId(df, shift_id_dict)
#         assignment_dict = {(assignment['machineid']): assignment for assignment in s2['tnt_org_machine_assignment']}
#         df = assign_org_unit_dept(df, assignment_dict)
#
#
#         # dm_shftwise table
#         df1 = pd.DataFrame(await getDMShiftDf(variables))
#         if df1.empty:
#             return {"status": "Required Data is not available"}
#
#         # dm_operator_machine_availability
#         df2 = pd.DataFrame(await getOperatorMachineAvailabilityDf(variables))
#         if df2.empty:
#             return {"status": "Required Data is not available"}
#
#         operator_tag_dict = await getEmpTagAssignData()
#         tenant_emp_dict = await tenant_employee_dict()
#         df3 = pd.DataFrame(await getoperatorSkillDict())
#        # To assign job_role
#         job_role_dict = await getOpertorJobRoleDict()
#         confige_code_dict = await getConfigeCodeDict()
#
#         separate_dfs = {name: group.sort_values(by='ts', ascending=True) for name, group in df.groupby(df['timestamp'].dt.date)}
#         workforce_availability_dict = {}
#
#         for date, df_date in separate_dfs.items():
#             workforce_availability_dict[date] = {}
#
#
#             # Iterate over hours for the current machine
#             for (tenant_id,shift_id,tag_id,machine_id), df_group in df_date.groupby([ df_date['tenantid'], df_date['shift_id'],df_date['tagid'],df_date['machineid']]):
#
#                 operator_id = assignOperatorToTag(df['tenantid'].iloc[0], tag_id, operator_tag_dict)
#                 job_role = getOperatorJobRole(operator_id, tenant_emp_dict, job_role_dict)
#                 salary = getOperatorSalary(tenant_emp_dict, operator_id, tenant_id)
#                 gender = getOperatorGender(tenant_emp_dict, operator_id, tenant_id)
#                 certification = getOperatorCertification(tenant_emp_dict, operator_id, tenant_id)
#                 experience = getOperatorTotalExperience(tenant_emp_dict, operator_id, tenant_id)
#                 Education = getOperatorEducation(tenant_emp_dict, confige_code_dict, operator_id, tenant_id)
#                 marital_status = getOperatorMaritalStatus(tenant_emp_dict, confige_code_dict, operator_id, tenant_id)
#                 skill = getOperatorSkills(df3, tenant_id, operator_id)
#
#                 proximity_in_time = fetchProximityInTime(df2, date, tenant_id, df_group['machineid'].iloc[-1], operator_id, shift_id)
#                 total_run_time = fetchRunTime(df1, date, tenant_id, machine_id, shift_id)
#                 workforce_utilization_per  =calculateWorkforceUtilization(proximity_in_time, total_run_time)
#
#                 datamart_dict_individual = {
#                     'date': date,
#                     'timestamp': df_group['ts'].iloc[-1],
#                     'tenantid': tenant_id,
#                     'machineid': machine_id,
#                     'edgeid': df_group['edgeid'].iloc[-1],
#                     'org_id': df_group['org_id'].iloc[-1],
#                     'unit_id': df_group['unit_id'].iloc[-1],
#                     'department_id': df_group['unit_id'].iloc[-1],
#                     'operator_id': operator_id,
#                     'proximity_in_time': round(proximity_in_time),
#                     'total_run_time': total_run_time,
#                     'workforce_utilization_per': workforce_utilization_per,
#                     'shift': shift_id,
#                     'job_role': job_role,
#                     'salary': salary,
#                     'gender': gender,
#                     'skill': skill,
#                     'certification': certification,
#                     'experience': experience,
#                     'education': Education,
#                     'marital_status': marital_status
#                 }
#                 #print(datamart_dict_individual)
#
#                 workforce_availability_dict[date][shift_id] = datamart_dict_individual
#         # Push individual machine actual production times to the database
#         individual_workforce_availability_dict = {}
#         for date, actual_prod_dict in workforce_availability_dict.items():
#             variable_name = f"workforce_availability_dict_{date}"
#             individual_workforce_availability_dict[variable_name] = actual_prod_dict
#
#             # Integrate the two lines here
#             formatted_data = [item[1] for item in actual_prod_dict.items()]
#             formatted_data = json.loads(json.dumps(formatted_data, default=convert_to_json_serializable))
#            # print(formatted_data)
#             mutation_objects = formatted_data
#             wfl0_workforce_availability_table = gql(
#                 """
#                    mutation MyMutation($objects: [wdm_workforce_utilization_insert_input!] = {}) {
#                       insert_wdm_workforce_utilization(objects: $objects) {
#                         affected_rows
#                       }
#                     }
#
#
#                 """
#             )
#             params = {"objects": mutation_objects}
#             await send_mutation(wfl0_workforce_availability_table, params)
#
#         return {
#             "status": "SUCCESS",
#
#         }
#     except Exception as e:
#         return {
#             "status": "FAILURE",
#             "error_message": str(e)
#         }
#














