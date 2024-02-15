import math
import statistics
from datetime import date,datetime,timedelta
import numpy as np
import pandas as pd
import time
import datetime as dt

def getBefore_n_HoursEpochTimestamp(num_of_hours):
    current_datetime = dt.datetime.now()
    before_datetime = current_datetime - dt.timedelta(hours=num_of_hours)
    before_datetime = before_datetime.replace(minute=0, second=0, microsecond=0)
    before_timestamp = int(before_datetime.timestamp()) * 1000
    return before_timestamp if before_timestamp else None


def getBefore_n_DaysEpochTimestamp(num_of_days):
    current_datetime = dt.datetime.now()
    before_datetime = current_datetime - dt.timedelta(days=num_of_days)
    before_datetime_midnight = before_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    before_timestamp = int(before_datetime_midnight.timestamp()) * 1000
    return before_timestamp if before_timestamp else None
def get_start_epoch_time_of_yesterdays_month():
    current_date = datetime.now()
    yesterday_date = current_date - timedelta(days=1)
    first_day_of_month = yesterday_date.replace(day=1)
    start_of_month_midnight = datetime.combine(first_day_of_month, datetime.min.time())
    start_epoch_timestamp = int(start_of_month_midnight.timestamp())
    return start_epoch_timestamp

def getBefore_n_MonthsEpochTimestamp(num_of_months):
    current_datetime = dt.datetime.now()
    before_datetime = current_datetime - dt.timedelta(days=current_datetime.day + 1)
    before_datetime = before_datetime.replace(day=1)
    before_datetime -= dt.timedelta(days=1)
    before_datetime -= dt.timedelta(days=before_datetime.day - 1)
    before_datetime -= dt.timedelta(weeks=4 * num_of_months)
    before_datetime_midnight = before_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    before_timestamp = int(before_datetime_midnight.timestamp()) * 1000
    return before_timestamp if before_timestamp else None
def get_realtime_millisecond_timestamp():
    return int(round(time.time() * 1000))

def get_one_hour_before_realtime_millisecond_timestamp():
    current_timestamp = int(round(time.time() * 1000))
    one_hour_before = current_timestamp - (60 * 60 * 1000)  # Subtract one hour in milliseconds
    return one_hour_before


# def convert_to_json_serializable(obj):
#     if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
#         return int(obj)
#     elif isinstance(obj, (dt.date, dt.datetime)):
#         return obj.strftime('%Y-%m-%d')
#     raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
def convert_to_json_serializable(obj):
    if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (dt.date, dt.datetime)):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, np.float64) and np.isnan(obj):
        return None
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")




def assignOperatorToTag(tenant_id, tag_id, operator_list):
    operator_mapping = {(item['tenantid'], item['tag_id'].lower()): item['employee_id'] for item in operator_list}

    tag_id = tag_id.lower()

    operator_id = operator_mapping.get((tenant_id, tag_id), None)

    return operator_id


def getFifteenMinOpertorTime(df, out_param, in_param):
    total_machine_in = 0

    df = df.reset_index(drop=True)
    start_time = df['timestamp'].iloc[0].replace(minute=0, second=0, microsecond=0)
    end_time = df['timestamp'].iloc[-1].replace(minute=14, second=59, microsecond=999999)

    inCount = df["state"].value_counts().get(in_param, 0)
    outCount = df["state"].value_counts().get(out_param, 0)

    # Case 1: (out) No out in the whole hour, only ins:
    if inCount >= 1 and outCount == 0:

        # For the states in the current dataframe:
        if inCount == 1:
            total_machine_in += (end_time - df['timestamp'].iloc[-1]).total_seconds() * 1000
        else:
            total_machine_in += (end_time - df['timestamp'].iloc[0]).total_seconds() * 1000
    # Case 2: No in only outs:
    if inCount == 0:
        total_machine_in += 0

    # Case 3: Where there might be series of (in then an out) or a series of in i.e. out!=0 and out
    if outCount >= 1 and inCount >= 1:
        in_series_started = False
        diff_in = 0
        in_series_end = None
        first_in_index = None

        for index, row in df.iterrows():
            state = row['state']

            if state == in_param and not in_series_started:
                # Found the start of an 'in' series, record its index
                in_series_started = True
                first_in_index = index

            elif (state == out_param and in_series_started) or (index == df.index[-1] and in_series_started):
                # Found the next 'out' after the 'in' series
                if first_in_index is not None:
                    diff_in += (df['timestamp'].iloc[index] - df['timestamp'].iloc[
                        first_in_index]).total_seconds() * 1000
                    in_series_started = False  # Reset the flag for the next 'in' series
                    in_series_end = index
                    first_in_index = None  # Reset first_in_index

        total_machine_in += diff_in

        # For the Last State:
        if df['state'].iloc[-1] == in_param:
            total_machine_in += (end_time - df['timestamp'].iloc[-1]).total_seconds() * 1000

    return total_machine_in

def getProximityOutTimeTotal(df, out_param, in_param):
    df = df.reset_index(drop=True)
    total_proximity_out_time = 0
    shift_start_time = df['shift_start'].iloc[-1]
    hr1, min1, sec1 = map(int, shift_start_time.split(':'))
    start_time = df['timestamp'].iloc[-1].replace(hour=hr1, minute=min1, second=sec1)
    shift_end_time = df['shift_end'].iloc[-1]
    hr2, min2, sec2 = map(int, shift_end_time.split(':'))
    end_time = df['timestamp'].iloc[-1].replace(hour=hr2, minute=min2, second=sec2)

    inCount = df["state"].value_counts().get(in_param, 0)
    outCount = df["state"].value_counts().get(out_param, 0)
    if inCount == 0 and outCount >= 1:
        total_proximity_out_time += (end_time - df['timestamp'].iloc[0]).total_seconds() * 1000
    if outCount == 0:
        total_proximity_out_time += 0

    if inCount >= 1 and outCount >= 1:
        out_series_started = False
        diff_out = 0
        out_series_end = None
        first_out_index = None

        for index, row in df.iterrows():
            state = row['state']

            if state == out_param and not out_series_started:
                out_series_started = True
                first_out_index = index
            elif (state == in_param and out_series_started) or (index == df.index[-1] and out_series_started):
                if first_out_index is not None:
                    diff_out += (df['timestamp'].iloc[index] - df['timestamp'].iloc[
                        first_out_index]).total_seconds() * 1000
                    out_series_started = False  # Reset the flag for the next 'Out' series
                    out_series_end = index
                    first_out_index = None  # Reset first_out_index
        total_proximity_out_time += diff_out
        # For the Last State:
        if df['state'].iloc[-1] == out_param:
            total_proximity_out_time += (end_time - df['timestamp'].iloc[-1]).total_seconds() * 1000
    # For the First State:
    if df['state'].iloc[0] == in_param:
        total_proximity_out_time += (df['timestamp'].iloc[0] - start_time).total_seconds() * 1000
    return total_proximity_out_time


def getProximityInTimeTotal(df, out_param, in_param):
    total_proximity_in_time = 0
    df = df.reset_index(drop=True)
    shift_start_time = df['shift_start'].iloc[-1]
    # Get hour, minute, and second from time string
    hr1, min1, sec1 = map(int, shift_start_time.split(':'))
    # Replace hour, minute, and second in the datetime object
    start_time = df['timestamp'].iloc[-1].replace(hour=hr1, minute=min1, second=sec1)
    shift_end_time = df['shift_end'].iloc[0]
    # Get hour, minute, and second from time string
    hr2, min2, sec2 = map(int, shift_end_time.split(':'))
    # Replace hour, minute, and second in the datetime object
    end_time = df['timestamp'].iloc[-1].replace(hour=hr2, minute=min2, second=sec2)
    inCount = df["state"].value_counts().get(in_param, 0)
    outCount = df["state"].value_counts().get(out_param, 0)
    # Case 1: (out) No out in the whole hour, only ins:
    if inCount >= 1 and outCount == 0:
        if inCount == 1:
            total_proximity_in_time += (end_time - df['timestamp'].iloc[-1]).total_seconds() * 1000
        else:
            total_proximity_in_time += (end_time - df['timestamp'].iloc[0]).total_seconds() * 1000
    # Case 2: No in only outs:
    if inCount == 0:
        total_proximity_in_time += 0
    # Case 3: Where there might be series of (in then an out) or a series of in i.e. out!=0 and out
    if outCount >= 1 and inCount >= 1:
        in_series_started = False
        diff_in = 0
        in_series_end = None
        first_in_index = None
        for index, row in df.iterrows():
            state = row['state']
            if state == in_param and not in_series_started:
                # Found the start of an 'in' series, record its index
                in_series_started = True
                first_in_index = index
            elif (state == out_param and in_series_started) or (index == df.index[-1] and in_series_started):
                # Found the next 'out' after the 'in' series
                if first_in_index is not None:
                    diff_in += (df['timestamp'].iloc[index] - df['timestamp'].iloc[
                        first_in_index]).total_seconds() * 1000
                    in_series_started = False  # Reset the flag for the next 'in' series
                    in_series_end = index
                    first_in_index = None  # Reset first_in_index
        total_proximity_in_time += diff_in
     # For the Last State:
        if df['state'].iloc[-1] == in_param:
            total_proximity_in_time += (end_time - df['timestamp'].iloc[-1]).total_seconds() * 1000
    # For the First State:
    if df['state'].iloc[0] == out_param:
        total_proximity_in_time += (df['timestamp'].iloc[0] - start_time).total_seconds() * 1000
    return total_proximity_in_time


def calculateEffectiveProximityTime(in_time, shift_start, shift_end):
    shift_start_time = datetime.strptime(shift_start, "%H:%M:%S").time()
    shift_end_time = datetime.strptime(shift_end, "%H:%M:%S").time()
    total_shift_time = (datetime.combine(datetime.today(), shift_end_time) -
                        datetime.combine(datetime.today(), shift_start_time)).total_seconds() * 1000
    effective_proximity_time = (in_time / total_shift_time) * 100
    return effective_proximity_time

def calculateProximityLossTimePercentage(machine_out_time, shift_start, shift_end):
    shift_start_time = datetime.strptime(shift_start, "%H:%M:%S").time()
    shift_end_time = datetime.strptime(shift_end, "%H:%M:%S").time()
    total_shift_time = (datetime.combine(datetime.today(), shift_end_time) -
                        datetime.combine(datetime.today(), shift_start_time)).total_seconds() * 1000
    proximity_time_Loss_per = (machine_out_time / total_shift_time) * 100

    return proximity_time_Loss_per



def getMachineProximityTimeLossPercent(proximity_out_time_total, planned_production_time):
    machine_proximity_time_loss_percent = 0

    if planned_production_time != 0:
        machine_proximity_time_loss_percent = (proximity_out_time_total / planned_production_time) * 100

    return machine_proximity_time_loss_percent


def getOperatorMachineInTimeTotal(df, out_param, in_param):
    total_machine_in = 0
    df = df.drop_duplicates(subset=['timestamp', 'state'])  # Drop duplicates based on timestamp and state
    df = df.reset_index(drop=True)
    if df.empty:
        return 0
    start_time = df['timestamp'].iloc[0].replace(minute=0, second=0, microsecond=0)
    end_time = df['timestamp'].iloc[-1].replace(minute=59, second=59, microsecond=999999)
    inCount = df["state"].value_counts().get(in_param, 0)
    outCount = df["state"].value_counts().get(out_param, 0)
    if inCount >= 1 and outCount == 0:
        if inCount == 1:
            total_machine_in += (end_time - df['timestamp'].iloc[-1]).total_seconds() * 1000
        else:
            total_machine_in += (end_time - df['timestamp'].iloc[0]).total_seconds() * 1000
    if outCount >= 1 and inCount >= 1:
        in_series_started = False
        diff_in = 0
        in_series_end = None
        first_in_index = None
        for index, row in df.iterrows():
            state = row['state']
            if state == in_param and not in_series_started:
                in_series_started = True
                first_in_index = index
            elif (state == out_param and in_series_started) or (index == df.index[-1] and in_series_started):
                if first_in_index is not None:
                    diff_in += (df['timestamp'].iloc[index] - df['timestamp'].iloc[first_in_index]).total_seconds() * 1000
                    in_series_started = False  # Reset the flag for the next 'in' series
                    in_series_end = index
                    first_in_index = None  # Reset first_in_index
        total_machine_in += diff_in
        # For the Last State:
        if df['state'].iloc[-1] == in_param:
            total_machine_in += (end_time - df['timestamp'].iloc[-1]).total_seconds() * 1000
    return total_machine_in if total_machine_in > 60000 else 0

def create_shift_dict(df):
    shift_dict = {}

    for _, row in df.iterrows():
        tenant_id = row['tenantid']
        shift_start = pd.to_datetime(row['shift_start_time']).time()
        shift_end = pd.to_datetime(row['shift_end_time']).time()
        shift_id = row['shift_name']

        if tenant_id not in shift_dict:
            shift_dict[tenant_id] = []

        # Exclude entries with 'General Shift'
        if shift_id != 'General Shift':
            shift_info = {
                'shift_start': shift_start,
                'shift_end': shift_end,
                'shift_id': shift_id
            }
            shift_dict[tenant_id].append(shift_info)
    return shift_dict

def AssignShiftId(df, shift_dict):
    timestamp = df['timestamp'].dt.time
    tenant_id = df['tenantid']

    shift_id_list = []

    for i, tenant_id_val in enumerate(tenant_id):
        if tenant_id_val in shift_dict:
            shift_info_list = shift_dict[tenant_id_val]

            for shift_info in shift_info_list:
                shift_start = shift_info['shift_start']
                shift_end = shift_info['shift_end']

                shift_start = shift_start.replace(second=0, microsecond=0)  # Ensure both are datetime.time objects
                shift_end = shift_end.replace(second=0, microsecond=0)

                if pd.notna(shift_start) and pd.notna(shift_end):

                    if shift_start > shift_end:
                        if timestamp.iloc[i] >= shift_start or timestamp.iloc[i] <= shift_end:
                            shift_id_list.append(shift_info['shift_id'])
                            break
                    else:
                        # Normal condition for shift_start <= shift_end
                        if shift_start <= timestamp.iloc[i] <= shift_end:
                            shift_id_list.append(shift_info['shift_id'])
                            break
            else:
                shift_id_list.append(None)
        else:
            shift_id_list.append(None)
    return shift_id_list

def addShiftStartAndShiftEnd(df, shift_dict):
    for shift in shift_dict:
        condition = (df['tenantid'] == shift['tenantid']) & (df['shift_id'] == shift['shift_name'])
        df.loc[condition, 'shift_start'] = shift['shift_start_time']
        df.loc[condition, 'shift_end'] = shift['shift_end_time']
    return df
def createDictTenantOperator(df):
    df.rename(columns={'tenantid': 'tenant_id'}, inplace=True)
    unique_operator_ids = df.groupby('tenant_id')['employee_id'].unique().to_dict()
    unique_operator_ids_dict = {key: value.tolist() for key, value in unique_operator_ids.items()}
    return unique_operator_ids_dict


def checkTagIdPresence(df, date, tenant_tag_dict):
    df['timestamp'] = pd.to_datetime(df['ts'], unit='ms', origin='unix')
    df.sort_values(by='timestamp').reset_index(drop=True, inplace=True)
    start_time = pd.to_datetime('00:00:00').time()
    end_time = pd.to_datetime('23:59:59').time()
    input_date = pd.to_datetime(date).date()
    date_filter = df['timestamp'].dt.date == input_date
    date_data = df[date_filter].copy()
    if date_data.empty:
        return 1
    slot_data = date_data[
        (date_data['timestamp'].dt.time >= start_time) &
        (date_data['timestamp'].dt.time <= end_time)
        ].copy()
    for tenant_id, tag_id_list in tenant_tag_dict.items():
        for tag_id in tag_id_list:
            condition = (
                    (slot_data['tenantid'] == tenant_id) &
                    (slot_data['tagid'] == tag_id)
            )
            if not slot_data[condition].empty:
                return 0
    return 1


def getOperatorName(operator_Name_Dict, req_operator_id, req_tenant_id):
    for employee_info in operator_Name_Dict:
        if "employee_id" in employee_info and "tenantid" in employee_info:
            operator_id = employee_info["employee_id"]
            tenant_id = employee_info["tenantid"]
            operator_info = next(
                (
                    operator
                    for operator in operator_Name_Dict
                    if operator.get("employee_id") == operator_id and operator.get("tenantid") == tenant_id
                ),
                None,
            )
            if operator_info and operator_id == req_operator_id and tenant_id == req_tenant_id:
                return f"{operator_info.get('first_name', '')} {operator_info.get('last_name', '')}"

    return None

def assign_org_unit_dept(df, assignment_dict):
    # Create new columns in telemetry_df
    df['org_id'] = None
    df['unit_id'] = None
    df['department_id'] = None

    # Iterate through the telemetry_df rows and update values based on the dictionary
    for index, row in df.iterrows():
        machine_id = row['machineid']
        if machine_id in assignment_dict:
            df.at[index, 'org_id'] = assignment_dict[machine_id]['org_id']
            df.at[index, 'unit_id'] = assignment_dict[machine_id]['unit']
            df.at[index, 'department_id'] = assignment_dict[machine_id]['department']
    return df


def createDictTenantOperator(df):
    df.rename(columns={'tenantid': 'tenant_id'}, inplace=True)
    unique_operator_ids = df.groupby('tenant_id')['tag_id'].unique().to_dict()
    unique_operator_ids_dict = {key: value.tolist() for key, value in unique_operator_ids.items()}
    return unique_operator_ids_dict

# As of now we take total working days in months are 26
def absenteeismRatioMonthly(df, tenant_id, operator_id):
    filtered_df = df[(df['tenantid'] == tenant_id) & (df['operator_id'] == operator_id) & (df['is_absent'] == 1)]
    absenteeism_count = len(filtered_df)
    absenteeism_ratio = absenteeism_count / 26

    return absenteeism_ratio

#As of now we take total working days in week are 6

def absenteeismRatioWeekly(df, tenant_id, operator_id):
    filtered_df = df[(df['tenantid'] == tenant_id) & (df['operator_id'] == operator_id) & (df['is_absent'] == 1)]
    absenteeism_count = len(filtered_df)
    absenteeism_ratio = absenteeism_count / 6
    return absenteeism_ratio


# This function is use for to calculate count  planned_workforce in shift  but now we are taking the planned_workfoce = total tenat emp


def getEndCustomer(batch_details_dict, date, hour, tenant_id, machine_id, operator_id):
    target_time = hour % 24
    operator_id_str = str(operator_id)
    filtered_batches = [batch for batch in batch_details_dict if
                        batch['batch_start_date'] == date and
                        batch['tenantid'] == tenant_id and
                        batch['machineid'] == machine_id and
                        batch['operator_id'] == operator_id_str]
    if not filtered_batches:
        return None
    for i, batch in enumerate(filtered_batches[:-1]):
        current_batch_time = datetime.strptime(batch['batch_start_time'], '%H:%M').hour
        next_batch_time = datetime.strptime(filtered_batches[i + 1]['batch_start_time'], '%H:%M').hour
        if current_batch_time <= target_time < next_batch_time or target_time == current_batch_time:
            return batch['end_customer_name']
        elif target_time == next_batch_time:
            return filtered_batches[i + 1]['end_customer_name']
    second_batch = filtered_batches[-1]
    second_batch_time = datetime.strptime(second_batch['batch_start_time'], '%H:%M').hour
    if target_time >= second_batch_time:
        return second_batch['end_customer_name']
    first_batch = filtered_batches[0]
    first_batch_time = datetime.strptime(first_batch['batch_start_time'], '%H:%M').hour
    if target_time < first_batch_time:
        return first_batch['end_customer_name']
    return None


def getPartId(batch_details_dict, date, hour, tenant_id, machine_id, operator_id):
    target_time = hour % 24
    operator_id_str = str(operator_id)
    filtered_batches = [batch for batch in batch_details_dict if
                        batch['batch_start_date'] == date and
                        batch['tenantid'] == tenant_id and
                        batch['machineid'] == machine_id and
                        batch['operator_id'] == operator_id_str]
    if not filtered_batches:
        return None
    for i, batch in enumerate(filtered_batches[:-1]):
        current_batch_time = datetime.strptime(batch['batch_start_time'], '%H:%M').hour
        next_batch_time = datetime.strptime(filtered_batches[i + 1]['batch_start_time'], '%H:%M').hour
        if current_batch_time <= target_time < next_batch_time or target_time == current_batch_time:
            return batch['part_id']
        elif target_time == next_batch_time:
            return filtered_batches[i + 1]['part_id']
    second_batch = filtered_batches[-1]
    second_batch_time = datetime.strptime(second_batch['batch_start_time'], '%H:%M').hour
    if target_time >= second_batch_time:
        return second_batch['part_id']
    first_batch = filtered_batches[0]
    first_batch_time = datetime.strptime(first_batch['batch_start_time'], '%H:%M').hour
    if target_time < first_batch_time:
        return first_batch['part_id']
    return None

#




# for operator_machine_availibilty Datamart
def getOperatorJobRole(operator_id, tenant_emp_dict, job_role_dict):
    operator_info = next((emp for emp in tenant_emp_dict if emp['employee_id'] == operator_id), None)
    if operator_info:
        job_role_info = next((role for role in job_role_dict if role['job_id'] == operator_info.get('job_role_id')), None)
        return job_role_info.get('job_role') if job_role_info else None
    return None

def getOperatorSkills(df, tenantid, operator_id):
    df = df[ (df['operator_id'] == operator_id) & (df['tenantid'] == tenantid)]
    if not df.empty:
        return df['skill'].iloc[0]
    else:
        return None



def getOperatorSalary(tenant_emp_dict, operator_id, tenantid):
    filtered_data = [item for item in tenant_emp_dict if item['employee_id'] == operator_id and item['tenantid'] == tenantid]
    return float(filtered_data[0]['monthly_ctc']) if filtered_data and filtered_data[0]['monthly_ctc'] else None

def getOperatorGender(tenant_emp_dict, operator_id, tenantid):
    filtered_data = [item for item in tenant_emp_dict if item['employee_id'] == operator_id and item['tenantid'] == tenantid]
    return filtered_data[0]['gender'] if filtered_data else None

def getOperatorCertification(tenant_emp_dict,operator_id,tenantid):
    filtered_data = [item for item in tenant_emp_dict if item['employee_id'] == operator_id and item['tenantid'] == tenantid]
    if filtered_data:
        return filtered_data[0]['certification']
    else:
        return None

def getOperatorTotalExperience(tenant_emp_dict, operator_id, tenantid):
    filtered_data = [item for item in tenant_emp_dict if item['employee_id'] == operator_id and item['tenantid'] == tenantid]
    return float(filtered_data[0]['total_exp_yr']) if filtered_data and filtered_data[0]['total_exp_yr'] else None


def getOperatorEducation(tenant_employee_dict, code_master_dict, operator_id, tenantid):
    filtered_data = [item for item in tenant_employee_dict if
                     item['employee_id'] == operator_id and item['tenantid'] == tenantid]
    if filtered_data:
        education_code = filtered_data[0]['education']
        education_code = int(education_code)
        education_name = None
        if education_code is not None:
            education_entry = next((entry for entry in code_master_dict if entry['code_id'] == education_code), None)
            if education_entry:
                education_name = education_entry['code']
        return education_name
    else:
        return None


def getOperatorMaritalStatus(tenant_employee_dict, code_master_dict, operator_id, tenantid):
    filtered_data = [item for item in tenant_employee_dict if
                     item['employee_id'] == operator_id and item['tenantid'] == tenantid]
    if filtered_data:
        marital_status_code = filtered_data[0].get('marital_status')
        if marital_status_code is not None:
            marital_status_code = int(marital_status_code)
            marital_status_entry = next(
                (entry for entry in code_master_dict if entry.get('code_id') == marital_status_code), None)
            if marital_status_entry:
                marital_status = marital_status_entry.get('code')
                return marital_status

    return None
def fetchProximityInTime(df, date, tenant_id, machine_id, operator_id, shift_id):
    date = date.strftime('%Y-%m-%d')
    conditions = (df['date'] == date) & (df['tenantid'] == tenant_id) & (df['machineid'] == machine_id) & (df['operator_id'] == operator_id) & (df['shift'] == shift_id)
    return df.loc[conditions, 'proximity_in_time_total'].iloc[0] if not df.loc[conditions].empty else None

# To featch planned production time  from dm_shiftwise
def fetchPlannedProductionTime(df, date, tenant_id, machine_id, shift_id):
    date = date.strftime('%Y-%m-%d')
    conditions = (df['date'] == date) & (df['tenantid'] == tenant_id) & (df['machineid'] == machine_id) & (df['shift_id'] == shift_id)
    return df.loc[conditions, 'planned_production_time'].iloc[0] if not df.loc[conditions].empty else None

# to calculate efficiency time by using proximity_in time and planned_production time
def calculateEfficiencyTime(proximity_in_time, planned_production_time):
    if planned_production_time == 0:
        return 0
    if proximity_in_time is None or planned_production_time is None:
        return None
    return (proximity_in_time / planned_production_time) * 100
# To featch the actual_production_time from dm_shiftwise
def fetchActualProductionTime(df, date, tenant_id, machine_id, shift_id):
    date = date.strftime('%Y-%m-%d')
    filtered_df = df[(df['date'] == date) & (df['tenantid'] == tenant_id) & (df['machineid'] == machine_id) & (df['shift_id'] == shift_id)]
    return filtered_df.iloc[0]['actual_production_time'] if not filtered_df.empty else None


def fetchTargetPart(df, date, tenant_id, machine_id, shift_id):
    date = date.strftime('%Y-%m-%d')
    conditions = (df['date'] == date) & (df['tenantid'] == tenant_id) & (df['machineid'] == machine_id) & (df['shift_id'] == shift_id)
    return df.loc[conditions, 'target_parts'].iloc[0] if not df.loc[conditions].empty else None

def fetchTotalPartsProduced(df, date, tenant_id, machine_id, shift_id):
    date = date.strftime('%Y-%m-%d')
    conditions = (df['date'] == date) & (df['tenantid'] == tenant_id) & (df['machineid'] == machine_id) & (df['shift_id'] == shift_id)
    return df.loc[conditions, 'total_parts_produced'].iloc[0] if not df.loc[conditions].empty else None

def fetchQualityPercentage(df, date, tenant_id, machine_id, shift_id):
    date = date.strftime('%Y-%m-%d')
    conditions = (df['date'] == date) & (df['tenantid'] == tenant_id) & (df['machineid'] == machine_id) & (df['shift_id'] == shift_id)
    return df.loc[conditions, 'quality_percent'].iloc[0] if not df.loc[conditions].empty else None


def calculateEfficiencyToProducedParts(total_parts_produced, target_parts):
    if pd.isna(total_parts_produced) or pd.isna(target_parts):
        return None
    if target_parts == 0:
        return 0
    efficiency = (total_parts_produced / target_parts) * 100
    return efficiency

# overall labor effectiveness
def getOle(time_efficiency, production_efficiency, quality_efficiency_per):
    ole = (time_efficiency / 100) * (production_efficiency / 100) * (quality_efficiency_per / 100) * 100
    return ole


# workfoce utilization


 # workforce utilization
def fetchRunTime(df, date, tenant_id, machine_id, shift_id):
    date = date.strftime('%Y-%m-%d')
    conditions = (df['date'] == date) & (df['tenantid'] == tenant_id) & (df['machineid'] == machine_id) & (df['shift_id'] == shift_id)
    return df.loc[conditions, 'total_machine_runtime'].iloc[0] if not df.loc[conditions].empty else None


def calculateWorkforceUtilization(proximity_in_time, total_run_time):
    if proximity_in_time is None or total_run_time is None:
        return None
    return (proximity_in_time / total_run_time) * 100 if total_run_time != 0 else 0

def getTotalavailableWorkforce(df):
    # Check if the DataFrame is empty
    if df.empty:
        return None
    else:
        # If tag_id starts with 'die', filter it out
        filtered_df = df[~df['tagid'].str.startswith('die')]
        # Return the count of unique tagids
        return filtered_df['tagid'].nunique()


def getTotalWorkforce(tenant_employees, department):

    actual_workforce = 0
    for entry in tenant_employees:
        if entry.get('department') == department:

            actual_workforce += 1
    return actual_workforce

def workforceAvailabilityPercentage(total_workforce, available_workforce):
    if total_workforce is None or available_workforce is None:
        return None
    return (available_workforce / total_workforce) * 100 if total_workforce != 0 else 0


def AvailableWorkforceByJobRole(df, job_role):
    filtered_df = df[(df['job_role'] == job_role) ]
    available_workforce = len(filtered_df['operator_id'].unique())
    return available_workforce
def AvailableWorkforceBySkill(df, skill):
    filtered_df = df[(df['skill'] == skill) ]
    available_workforce = len(filtered_df['operator_id'].unique())
    return available_workforce

def getUnavailableWorkforce(total_workforce, available_workforce):
    if total_workforce is None or available_workforce is None:
        return None
    return total_workforce - available_workforce


def getUnavailableWorkforcePer(total_workforce, unavailable_workforce):
    if total_workforce is None or unavailable_workforce is None:
        return None
    return (unavailable_workforce / total_workforce) * 100 if total_workforce != 0 else 0


def getUnavailableWorkforceByJobRole(total_workforce, available_workforce_by_job_role):
    if total_workforce is None or available_workforce_by_job_role is None:
        return None
    return total_workforce - available_workforce_by_job_role


def getAvailableWorkforceByJobRolePer(total_workforce, available_workforce_by_job_role):
    if total_workforce is None or total_workforce == 0:
        return None
    return (available_workforce_by_job_role / total_workforce) * 100
def getUnAvailableWorkforceByJobRolePer(total_workforce, unavailable_workforce_by_job_role):
    if total_workforce is None or total_workforce == 0:
        return None
    return (unavailable_workforce_by_job_role / total_workforce) * 100

def getUnavailableWorkforceByskill(total_workforce, available_workforce_by_skill):
    if total_workforce is None or available_workforce_by_skill is None:
        return None
    return total_workforce - available_workforce_by_skill


def getavailableWorkforceBySkillPer(total_workforce, available_workforce_by_skill):
    if total_workforce is None or total_workforce == 0:
        return None
    return (available_workforce_by_skill / total_workforce) * 100


def getUnavailableWorkforceBySkillPer(total_workforce, unavailable_workforce_by_skill):
    if total_workforce is None or total_workforce == 0:
        return None
    return (unavailable_workforce_by_skill / total_workforce) * 100


def fetchJobRole(df, operator_id):
    if df.empty or operator_id is None:
        return None
    conditions = (df['operator_id'] == operator_id)
    return df.loc[conditions, 'job_role'].iloc[0] if not df.loc[conditions].empty else None


def fetchSkill(df, operator_id):
    if df.empty or operator_id is None:
        return None
    conditions = (df['operator_id'] == operator_id)
    return df.loc[conditions, 'skill'].iloc[0] if not df.loc[conditions].empty else None

def fetchExperience(df, operator_id):
    if df.empty or operator_id is None:
        return None
    conditions = (df['operator_id'] == operator_id)
    return df.loc[conditions, 'experience'].iloc[0] if not df.loc[conditions].empty else None


def fetchEducation(df, operator_id):
    if df.empty or operator_id is None:
        return None
    conditions = (df['operator_id'] == operator_id)
    return df.loc[conditions, 'education'].iloc[0] if not df.loc[conditions].empty else None


def fetchSalary(df,operator_id):
    if df.empty or operator_id is None:
        return None
    conditions = (df['operator_id'] == operator_id)
    return df.loc[conditions, 'salary'].iloc[0] if not df.loc[conditions].empty else None


def fetchGender(df,operator_id):
    if df.empty or operator_id is None:
        return None
    conditions = (df['operator_id'] == operator_id)
    return df.loc[conditions, 'gender'].iloc[0] if not df.loc[conditions].empty else None
def fetchCertification(df,operator_id):
    if df.empty or operator_id is None:
        return None
    conditions = (df['operator_id'] == operator_id)
    return df.loc[conditions, 'certification'].iloc[0] if not df.loc[conditions].empty else None







