#from wsgiref import simple_server
from flask import Flask, request, render_template, jsonify, session
from flask import Response
import os
from flask_cors import CORS, cross_origin
from controller.project_controller.projects.WaferFaultDetection_new.prediction_Validation_Insertion import \
    PredictionValidation
from controller.project_controller.projects.WaferFaultDetection_new.trainingModel import TrainingModel
from controller.project_controller.projects.WaferFaultDetection_new.training_Validation_Insertion import \
    TrainingValidation
import flask_monitoringdashboard as dashboard
from controller.project_controller.projects.WaferFaultDetection_new.predictFromModel import Prediction
from logging_layer.logger.log_request import LogRequest
from data_access_layer.mongo_db.mongo_db_atlas import MongoDBOperation
from project_library_layer.initializer.initializer import Initializer
import json
import uuid
from threading import Thread
#os.putenv('LANG', 'en_US.UTF-8')
#os.putenv('LC_ALL', 'en_US.UTF-8')

#app = Flask(__name__)
#dashboard.bind(app)
#CORS(app)
from entity_layer.registration.registration import Register
from entity_layer.project.project import Project
from entity_layer.project.project_configuration import ProjectConfiguration

class TrainModelThread(Thread):

    def __init__(self,project_id,executed_by,execution_id):
        Thread.__init__(self)
        self.project_id=project_id
        self.executed_by=executed_by
        self.execution_id=execution_id
        self.mongo_db=MongoDBOperation()
        self.initialize=Initializer()

    def run(self):
        try:
            socket_io.emit('newnumber', {'number': number}, namespace='/test')
            is_training_already_running = False
            training_thread_database_name=self.initialize.get_training_thread_database_name()
            thread_status_collection_name=self.initialize.get_thread_status_collection_name()
            max_status_id=None
            if self.mongo_db.is_database_present(self.mongo_db.get_database_client_object(),training_thread_database_name):
                database_obj=self.mongo_db.create_database(self.mongo_db.get_database_client_object(),training_thread_database_name)
                if self.mongo_db.is_collection_present(thread_status_collection_name,database_obj):
                    max_status_id=self.mongo_db.get_max_value_of_column(training_thread_database_name,
                                                          thread_status_collection_name,
                                                          {'project_id':self.project_id},
                                                          'status_id')

                    response=self.mongo_db.get_record(training_thread_database_name,
                                                      thread_status_collection_name,
                                                      {'project_id':self.project_id,'status_id':max_status_id})

                    if 'is_running' in response:
                        is_training_already_running = response['is_running']

            if is_training_already_running:
                return {'status':True,'message':"Training is already in progress please wait..."}

            if max_status_id is None:
                status_id=1
            else:
                status_id=max_status_id+1
            record= {'project_id':self.project_id,
                     'execution_id':self.execution_id,
                     'executed_by':self.executed_by,
                     'status_id':status_id,
                     'is_running':True
                      }
            self.mongo_db.insert_record_in_collection(training_thread_database_name,
                                                      thread_status_collection_name,
                                                      record
                                                      )
            training_model(project_id=self.project_id, executed_by=self.executed_by, execution_id=self.execution_id)
            self.mongo_db.update_record_in_collection(training_thread_database_name,
                                                      thread_status_collection_name,
                                                      {'status_id':status_id}, {'is_running':False})
            response={'message':'Training completed','status':True,'message_status': 'info', 'project_id': self.project_id}
            return jsonify(response)
        except Exception as e:
            raise e


class PredictFromModelThread(Thread):

    def __init__(self, project_id, executed_by, execution_id):
        Thread.__init__(self)
        self.project_id = project_id
        self.executed_by = execution_id
        self.execution_id = execution_id
        self.initialize=Initializer()
        self.mongo_db=MongoDBOperation()

    def run(self):

        try:
            is_prediction_already_running = False
            prediction_thread_database_name = self.initialize.get_prediction_thread_database_name()
            thread_status_collection_name = self.initialize.get_thread_status_collection_name()
            max_status_id = None
            if self.mongo_db.is_database_present(self.mongo_db.get_database_client_object(),prediction_thread_database_name):
                database_obj = self.mongo_db.create_database(self.mongo_db.get_database_client_object(),
                                                             prediction_thread_database_name)
                if self.mongo_db.is_collection_present(thread_status_collection_name, database_obj):
                    max_status_id = self.mongo_db.get_max_value_of_column(prediction_thread_database_name,
                                                                          thread_status_collection_name,
                                                                          {'project_id': self.project_id},
                                                                          'status_id')

                    response = self.mongo_db.get_record(prediction_thread_database_name,
                                                        thread_status_collection_name,
                                                        {'project_id': self.project_id, 'status_id': max_status_id})

                    if 'is_running' in response:
                        is_prediction_already_running = response['is_running']

            if is_prediction_already_running:
                return {'status': True, 'message': "Training is already in progress please wait..."}

            if max_status_id is None:
                status_id = 1
            else:
                status_id = max_status_id + 1
            record = {'project_id': self.project_id,
                      'execution_id': self.execution_id,
                      'executed_by': self.executed_by,
                      'status_id': status_id,
                      'is_running': True
                      }
            self.mongo_db.insert_record_in_collection(prediction_thread_database_name,
                                                      thread_status_collection_name,
                                                      record
                                                      )
            prediction_from_model(project_id=self.project_id, executed_by=self.executed_by, execution_id=self.execution_id)
            self.mongo_db.update_record_in_collection(prediction_thread_database_name,
                                                      thread_status_collection_name,
                                                      {'status_id': status_id}, {'is_running': False})
            response = {'message': 'Prediction completed', 'status': True, 'message_status': 'info',
                        'project_id': self.project_id}
            return jsonify(response)
        except Exception as e:
            raise e


def prediction_from_model(project_id,executed_by,execution_id):
    try:
        project_detail = Project()
        project_config = ProjectConfiguration()

        if project_id is None:
            raise Exception("Project id not found")
        project_detail = project_detail.get_project_detail(project_id=project_id)
        if not project_detail['status']:

            return project_detail.update(
                {'message_status': 'info', 'project_id': project_id})

        project_config_detail = project_config.get_project_configuration_detail(project_id=project_id)
        if not project_config_detail['status']:

            return project_config_detail.update(
                                       {'message_status': 'info', 'project_id': project_id})
        if 'project_config_detail' in project_config_detail:
            project_config_detail = project_config_detail['project_config_detail']
        if project_config_detail is None:
            response = {'status': False, 'message': 'project configuration not found',
                        'message_status': 'info', 'project_id': project_id}

            return response
        prediction_file_path = Initializer().get_prediction_batch_file_path(project_id=project_id)
        cloud_storage = None
        if 'cloud_storage' in project_config_detail:
            cloud_storage = project_config_detail['cloud_storage']
        if cloud_storage is None:
            result = {'status': False,
                      'message': 'Cloud Storage location not found',
                      'message_status': 'info', 'project_id': project_id}

            return result
        pred_val = PredictionValidation(project_id,
                                        prediction_file_path,
                                        executed_by,
                                        execution_id,
                                        cloud_storage
                                        )  # object initialization

        pred_val.prediction_validation()  # calling the training_validation function

        pred = Prediction(project_id,
                          executed_by,
                          execution_id,
                          cloud_storage)  # object initialization
        prediction_generated_file = pred.prediction_from_model()  # training the model for the files in the table
        response = {'status': True,
                    'message': 'Prediction completed at path {}'.format(prediction_generated_file),
                    'message_status': 'info', 'project_id': project_id}
        return response
    except Exception as e:
        raise e



def training_model(project_id, executed_by, execution_id):
    try:
        project_detail = Project()
        project_config = ProjectConfiguration()

        if project_id is None:
            raise Exception("Project id not found")
        project_detail = project_detail.get_project_detail(project_id=project_id)
        if not project_detail['status']:
            return project_detail.update(
                {'message_status': 'info', 'project_id': project_id})

        project_config_detail = project_config.get_project_configuration_detail(project_id=project_id)
        if not project_config_detail['status']:
            return project_config_detail.update(
                {'message_status': 'info', 'project_id': project_id})
        if 'project_config_detail' in project_config_detail:
            project_config_detail = project_config_detail['project_config_detail']
        if project_config_detail is None:
            response = {'status': False, 'message': 'project configuration not found',
                        'message_status': 'info', 'project_id': project_id}

            return response
        training_file_path = Initializer().get_training_batch_file_path(project_id=project_id)
        cloud_storage = None
        if 'cloud_storage' in project_config_detail:
            cloud_storage = project_config_detail['cloud_storage']
        if cloud_storage is None:
            result = {'status': False,
                      'message': 'Cloud Storage location not found',
                      'message_status': 'info', 'project_id': project_id}

            return result
        train_val_obj = TrainingValidation(project_id,
                                           training_file_path,
                                           executed_by,
                                           execution_id,
                                           cloud_storage
                                           )  # object initialization

        train_val_obj.train_validation()  # calling the training_validation function

        trainModelObj = TrainingModel(project_id,
                                      executed_by,
                                      execution_id,
                                      cloud_storage)  # object initialization
        trainModelObj.training_model()  # training the model for the files in the table

        response = {'status': True, 'message': 'Training completed successfully',
                    'message_status': 'info', 'project_id': project_id}
        return response
    except Exception as e:
        raise e


class WaferFaultDetectionProjectController:
    def __init__(self):
        self.registration_obj=Register()
        self.project_detail=Project()
        self.project_config=ProjectConfiguration()
        self.WRITE="WRITE"
        self.READ="READ"

    def predictRouteClient(self):
        project_id=None
        try:
            log_writer = LogRequest(executed_by=None, execution_id=str(uuid.uuid4()))
            try:
                #log_writer = LogRequest(executed_by=None, execution_id=str(uuid.uuid4()))
                if 'email_address' in session:
                    log_writer.executed_by = session['email_address']
                    log_writer.log_start(request)
                    result = self.registration_obj.validate_access(session['email_address'], operation_type=self.WRITE)
                    if not result['status']:
                        log_writer.log_stop(result)
                        return render_template('/project/wafer_fault_detection.html', context=result.update({'message_status':'info','project_id':project_id}))

                    project_id = int(request.args.get('project_id'))
                    response={}
                    PredictFromModelThread(project_id=project_id, executed_by=log_writer.executed_by,
                                     execution_id=log_writer.execution_id)

                    return render_template('/project/wafer_fault_detection.html',
                                           context=response.update({'message_status': 'info', 'project_id': project_id}))

            except Exception as e:
                return jsonify({'status':False,
                                'message':str(e)
                                   ,'message_status':'info','project_id':project_id})

        except Exception as e:
            return jsonify({'status': False,
                    'message': str(e)
                       , 'message_status': 'info', 'project_id': project_id})

    def trainRouteClient(self):
        project_id = None
        try:
            log_writer = LogRequest(executed_by=None, execution_id=str(uuid.uuid4()))
            project_id = int(request.args.get('project_id'))
            try:
                # log_writer = LogRequest(executed_by=None, execution_id=str(uuid.uuid4()))
                if 'email_address' in session:
                    log_writer.executed_by = session['email_address']
                    log_writer.log_start(request)
                    result = self.registration_obj.validate_access(session['email_address'], operation_type=self.WRITE)
                    if not result['status']:
                        log_writer.log_stop(result)
                        return render_template('/project/wafer_fault_detection.html', context=result.update(
                            {'message_status': 'info', 'project_id': project_id}))
                    result={}
                    #result=training_model(project_id,execution_id=log_writer.execution_id,
                    #                      executed_by=log_writer.executed_by)
                    #

                    train_model=TrainModelThread(project_id=project_id,executed_by=log_writer.executed_by,execution_id=log_writer.execution_id)
                    train_model.start()
                    result.update({'message':'Training started. keep execution_id[{}] to track'.format(log_writer.execution_id),
                                                                  'message_status': 'info', 'project_id': project_id})
                    log_writer.log_stop(result)
                    return render_template('/project/wafer_fault_detection.html',context=result)

            except Exception as e:
                return jsonify({'status':False,
                                'message':str(e)
                                   ,'message_status':'info','project_id':project_id})

        except Exception as e:
            return jsonify({'status': False,
                    'message': str(e)
                       , 'message_status': 'info', 'project_id': project_id})


"""   
    def predictRouteClient(self):
        project_id=None
        try:
            log_writer = LogRequest(executed_by=None, execution_id=str(uuid.uuid4()))
            try:
                #log_writer = LogRequest(executed_by=None, execution_id=str(uuid.uuid4()))
                if 'email_address' in session:
                    log_writer.executed_by = session['email_address']
                    log_writer.log_start(request)
                    result = self.registration_obj.validate_access(session['email_address'], operation_type=self.WRITE)
                    if not result['status']:
                        log_writer.log_stop(result)
                        return render_template('/project/wafer_fault_detection.html', context=result.update({'message_status':'info','project_id':project_id}))
                    initial = Initializer()
                    project_id = int(request.args.get('project_id'))
                    if project_id is None:
                        raise Exception("Project id not found")
                    project_detail = self.project_detail.get_project_detail(project_id=project_id)
                    if not project_detail['status']:
                        log_writer.log_stop(project_detail)
                        return render_template('/project/wafer_fault_detection.html', context=project_detail.update(
                            {'message_status': 'info', 'project_id': project_id}))

                    project_config_detail = self.project_config.get_project_configuration_detail(project_id=project_id)
                    if not project_config_detail['status']:
                        log_writer.log_stop(project_config_detail)
                        return render_template('/project/wafer_fault_detection.html',
                                               context=project_config_detail.update({'message_status': 'info', 'project_id': project_id}))
                    if 'project_config_detail' in project_config_detail:
                        project_config_detail=project_config_detail['project_config_detail']
                    if project_config_detail is None:
                        response={'status':False,'message':'project configuration not found',
                                  'message_status': 'info', 'project_id': project_id}
                        log_writer.log_stop(response)
                        return render_template('/project/wafer_fault_detection.html', context=response)
                    prediction_file_path = initial.get_prediction_batch_file_path(project_id=project_id)
                    cloud_storage = None
                    if 'cloud_storage' in project_config_detail:
                        cloud_storage = project_config_detail['cloud_storage']
                    if cloud_storage is None:
                        result = {'status': False,
                                  'message': 'Cloud Storage location not found',
                                  'message_status': 'info', 'project_id': project_id}
                        log_writer.log_stop(result)
                        return render_template('/project/wafer_fault_detection.html', context=result)
                    pred_val = pred_validation(project_id,
                                               prediction_file_path,
                                               log_writer.executed_by,
                                               log_writer.execution_id,
                                               cloud_storage
                                               )  # object initialization

                    pred_val.prediction_validation()  # calling the training_validation function

                    pred = prediction(project_id,
                                      log_writer.executed_by,
                                      log_writer.execution_id,
                                      cloud_storage)  # object initialization
                    prediction_generated_file=pred.predictionFromModel()  # training the model for the files in the table
                    response = {'status': True,
                                'message': 'Prediction completed at path {}'.format(prediction_generated_file),
                                'message_status': 'info', 'project_id': project_id}
                    log_writer.log_stop(response)
                    return render_template('/project/wafer_fault_detection.html', context=response)
            except ValueError as val_error:
                response = {'status': False, 'message_status': 'info',
                            'message': 'Prediction Failed due to {}'.format(str(val_error)),'project_id':project_id}
                log_writer.log_stop(response)
                return render_template('/project/wafer_fault_detection.html', context=response)

            except KeyError as key_error:
                response = {'status': False, 'message_status': 'info',
                            'message': 'Prediction Failed due to {}'.format(str(key_error)),'project_id':project_id}
                log_writer.log_stop(response)
                return render_template('/project/wafer_fault_detection.html', context=response)

            except Exception as e:
                response = {'status': False, 'message_status': 'info',
                            'message': 'Prediction Failed due to {}'.format(str(e)),'project_id':project_id}
                log_writer.log_stop(response)
                return render_template('/project/wafer_fault_detection.html', context=response)

        except Exception as e:
            response = {'status': False, 'message_status': 'info',
                        'message': 'Prediction Failed due to {}'.format(str(e)),'project_id':project_id}
            return render_template('/project/wafer_fault_detection.html', context=response)


    def trainRouteClient(self):
        project_id=None
        try:
            log_writer = LogRequest(executed_by=None, execution_id=str(uuid.uuid4()))
            project_id = int(request.args.get('project_id'))
            try:
                #log_writer = LogRequest(executed_by=None, execution_id=str(uuid.uuid4()))
                if 'email_address' in session:
                    log_writer.executed_by = session['email_address']
                    log_writer.log_start(request)
                    result = self.registration_obj.validate_access(session['email_address'], operation_type=self.WRITE)
                    if not result['status']:
                        log_writer.log_stop(result)
                        return render_template('/project/wafer_fault_detection.html', context=result.update({'message_status':'info','project_id':project_id}))
                    initial = Initializer()
                    project_id=int(request.args.get('project_id'))
                    if project_id is None:
                        raise Exception("Project id not found")
                    project_detail = self.project_detail.get_project_detail(project_id=project_id)
                    if not project_detail['status']:
                        log_writer.log_stop(project_detail)
                        return render_template('/project/wafer_fault_detection.html', context=project_detail.update({'message_status':'info','project_id':project_id}))
                    project_config_detail = self.project_config.get_project_configuration_detail(project_id=project_id)
                    if not project_config_detail['status']:
                        log_writer.log_stop(project_config_detail)
                        return render_template('/project/wafer_fault_detection.html', context=project_config_detail.update({'message_status':'info','project_id':project_id}))
                    if 'project_config_detail' in project_config_detail:
                        project_config_detail=project_config_detail['project_config_detail']
                    if project_config_detail is None:
                        response={'status':False,'message':'project configuration not found',
                                  'message_status':'info','project_id':project_id}
                        log_writer.log_stop(response)
                        return render_template('/project/wafer_fault_detection.html', context=response)
                    training_file_path = initial.get_training_batch_file_path(project_id=project_id)
                    cloud_storage = None
                    if 'cloud_storage' in project_config_detail:
                        cloud_storage = project_config_detail['cloud_storage']
                    if cloud_storage is None:
                        result = {'status': False,
                                  'message': 'Cloud Storage location not found',
                                  'message_status': 'info', 'project_id': project_id
                                  }
                        log_writer.log_stop(result)
                        return render_template('/project/wafer_fault_detection.html', context=result)

                    train_val_obj = train_validation(project_id,
                                                     training_file_path,
                                                     log_writer.executed_by,
                                                     log_writer.execution_id,
                                                     cloud_storage
                                                     )  # object initialization

                    train_val_obj.train_validation()  # calling the training_validation function

                    trainModelObj = trainModel(project_id,
                                                log_writer.executed_by,
                                                log_writer.execution_id,
                                                cloud_storage)  # object initialization
                    trainModelObj.trainingModel()  # training the model for the files in the table

                    response={'status':True,'message':'Training completed successfully',
                              'message_status':'info','project_id':project_id}
                    log_writer.log_stop(response)
                    return render_template('/project/wafer_fault_detection.html', context=response)
            except ValueError as val_error:

                response = {'status': False, 'message': 'Training Failed due to {}'.format(str(val_error)),
                            'message_status':'info','project_id':project_id}
                log_writer.log_stop(response)
                return render_template('/project/wafer_fault_detection.html', context=response)

            except KeyError as key_error:
                response = {'status': False, 'message': 'Training Failed due to {}'.format(str(key_error)),
                            'message_status':'info','project_id':project_id}
                log_writer.log_stop(response)
                return render_template('/project/wafer_fault_detection.html', context=response)

            except Exception as e:
                response = {'status': False, 'message': 'Training Failed due to {}'.format(str(e)),
                            'message_status':'info','project_id':project_id}
                log_writer.log_stop(response)
                return render_template('/project/wafer_fault_detection.html', context=response)
        except Exception as e:
            response = {'status': False, 'message': 'Training Failed due to {}'.format(str(e)),
                        'message_status':'info','project_id':project_id}
            return render_template('/project/wafer_fault_detection.html', context=response)

"""
        #return Response("Training successfull!!")


"""
port = int(os.getenv("PORT",5000))
if __name__ == "__main__":
    host = '0.0.0.0'
    #port = 5000
    httpd = simple_server.make_server(host, port, app)
    # print("Serving on %s %d" % (host, port))
    httpd.serve_forever()
"""
