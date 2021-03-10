# import the os module
import os
from datetime import datetime


def fun_main_dir_path():
    get_dir_path = os.getcwd()
    if not os.path.exists(get_dir_path + '/' + "sample_data_download"):
        os.mkdir(get_dir_path + '/' + "sample_data_download")
        return os.getcwd()+'/'+"sample_data_download"
    else:
        return get_dir_path+'/'+"sample_data_download"


main_folder_list = ['mldb','swcm','cndb','cedp']
# file_name = 'MLDB_18022021_1'
# file_name_new = 'SCAN_DATA_AIC_ISO_TAG_20210221_1'


def h1_folder(main_dir_path1, h_folder_name):
    if not os.path.exists(main_dir_path1 + '/' + h_folder_name):
        os.mkdir(main_dir_path1 + '/' + h_folder_name)
        # os.chdir(main_dir_path1 + '/' + h_folder_name)
        main_dir_path2 = main_dir_path1 + '/' + h_folder_name  # os.getcwd()
    else:
        main_dir_path2 = main_dir_path1 + '/' + h_folder_name  # os.getcwd()
    return main_dir_path2


def fun_week_no(day_no):
    import math
    no = math.ceil(day_no/7)
    return 'week-{0}'.format(no)


def file_mapped_folder(file_name):
    main_dir_path = fun_main_dir_path()
    # check higher level folder MLDB,CNDB,SWCM
    file_name_detail = '_'.join(file_name.split('_')[:-2])
    h_folder_name_h = ''
    if 'AIC' in file_name_detail or 'BFI' in file_name_detail:
        h_folder_name = 'cedp'
        if 'AIC' in file_name_detail:
            h_folder_name_h = 'aic'
        if 'BFI' in file_name_detail:
            h_folder_name_h = 'bfi'

    # h_folder_name = '_'.join(file_name.split('_')[:-2])
    h1_folder_date = file_name.split('_')[-2]
    date_obj = datetime.strptime(h1_folder_date, '%Y%m%d')
    if h_folder_name in main_folder_list:
        if not os.path.exists(main_dir_path+'/'+h_folder_name):
            os.mkdir(main_dir_path+'/'+h_folder_name)
            # os.chdir(main_dir_path + '/' + h_folder_name)
            main_dir_path1 = main_dir_path + '/' + h_folder_name #os.getcwd()
            if h_folder_name_h:# new layer
                main_dir_path1 = h1_folder(main_dir_path1, h_folder_name_h)
            h_folder_name = date_obj.strftime('%m')
            if not os.path.exists(main_dir_path1+'/'+h_folder_name):
                os.mkdir(main_dir_path1+'/'+h_folder_name)
                # os.chdir(main_dir_path1 + '/' + h_folder_name)
                main_dir_path2 = main_dir_path1 + '/' + h_folder_name #os.getcwd()
                day_no = date_obj.day
                week_folder_name = fun_week_no(day_no)
                if not os.path.exists(main_dir_path2+'/'+week_folder_name):
                    os.mkdir(main_dir_path2+'/'+week_folder_name)
                    # os.chdir(main_dir_path2 + '/' + week_folder_name)
                    final_path = main_dir_path2 + '/' + week_folder_name #os.getcwd()
                    return final_path
                else:
                    # os.chdir(main_dir_path2 + '/' + week_folder_name)
                    final_path = main_dir_path2 + '/' + week_folder_name #os.getcwd()
                    return final_path
            else:
                # os.chdir(main_dir_path1 + '/' + h_folder_name)
                main_dir_path2 = main_dir_path1 + '/' + h_folder_name #os.getcwd()
                day_no = date_obj.day
                week_folder_name = fun_week_no(day_no)
                if not os.path.exists(main_dir_path2+'/'+week_folder_name):
                    os.mkdir(main_dir_path2+'/'+week_folder_name)
                    # os.chdir(main_dir_path2 + '/' + week_folder_name)
                    final_path = main_dir_path2 + '/' + week_folder_name #os.getcwd()
                    return final_path
                else:
                    # os.chdir(main_dir_path2 + '/' + week_folder_name)
                    final_path = main_dir_path2 + '/' + week_folder_name #os.getcwd()
                    return final_path
        else:
            # os.chdir(main_dir_path+'/'+h_folder_name)
            main_dir_path1 = main_dir_path + '/' + h_folder_name  # os.getcwd()
            if h_folder_name_h:# new layer
                main_dir_path1 = h1_folder(main_dir_path1, h_folder_name_h)
            h_folder_name = date_obj.strftime('%m')
            if not os.path.exists(main_dir_path1+'/'+h_folder_name):
                os.mkdir(main_dir_path1+'/'+h_folder_name)
                # os.chdir(main_dir_path1 + '/' + h_folder_name)
                main_dir_path2 = main_dir_path1 + '/' + h_folder_name #os.getcwd()
                day_no = date_obj.day
                week_folder_name = fun_week_no(day_no)
                if not os.path.exists(main_dir_path2+'/'+week_folder_name):
                    os.mkdir(main_dir_path2+'/'+week_folder_name)
                    # os.chdir(main_dir_path2 + '/' + week_folder_name)
                    final_path = main_dir_path2 + '/' + week_folder_name #os.getcwd()
                    return final_path
                else:
                    # os.chdir(main_dir_path2 + '/' + week_folder_name)
                    final_path = main_dir_path2 + '/' + week_folder_name #os.getcwd()
                    return final_path
            else:
                # os.chdir(main_dir_path1 + '/' + h_folder_name)
                main_dir_path2 = main_dir_path1 + '/' + h_folder_name #os.getcwd()
                day_no = date_obj.day
                week_folder_name = fun_week_no(day_no)
                if not os.path.exists(main_dir_path2+'/'+week_folder_name):
                    os.mkdir(main_dir_path2+'/'+week_folder_name)
                    # os.chdir(main_dir_path2 + '/' + week_folder_name)
                    final_path = main_dir_path2 + '/' + week_folder_name #os.getcwd()
                    return final_path
                else:
                    # os.chdir(main_dir_path2 + '/' + week_folder_name)
                    final_path = main_dir_path2 + '/' + week_folder_name #os.getcwd()
                    return final_path