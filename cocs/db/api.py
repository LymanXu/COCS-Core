import datetime

from cocs.common import exceptions
from cocs.db.basedao import BaseDAO
from cocs.db import models


class ChainDao(BaseDAO):

    def create_chain(self, chain_dict):
        if not chain_dict.get('name'):
            raise exceptions.ParamMissing('name')

        if self.get_chain_by_name(chain_dict['name']):
            return

        return self.create_resource(models.Chain, chain_dict)

    def delete_chain(self, chain_name):
        try:
            result = self.get_chain_by_name(chain_name)
            if not result:
                return

            self.delete_resource(models.Chain, result['id'])
        except Exception as e:
            raise exceptions.ResourceInUsing(models.Chain,
                                             chain_name, e)

    def update_chain(self, chain_name, update_dict):
        resu = self.get_chain_by_name(chain_name)
        if not resu:
            return

        return self.update_resource(models.Chain, resu['id'], update_dict)

    def get_chain_by_id(self, chain_id):
        return self.get_resource(models.Chain, chain_id)

    def get_chain_by_name(self, chain_name):
        filter_dict = {'name': chain_name}
        return self.get_resource_by_attr(models.Chain, filter_dict)

    def list_chain_by_attr(self, filter_dict):
        return self.list_resources_by_attr(models.Chain, filter_dict)


class ServiceDao(BaseDAO):

    def get_service_by_id(self, service_id):
        return self.get_resource(models.Service, service_id)

    def get_service_by_name(self, service_name):
        filter_dict = {'name': service_name}
        return self.get_resource_by_attr(models.Service, filter_dict)

    def create_service(self, service_dict):
        if not service_dict.get('name'):
            raise exceptions.ParamMissing('name')

        if self.get_service_by_name(service_dict['name']):
            return

        return self.create_resource(models.Service, service_dict)

    def delete_service(self, service_name):
        try:
            result = self.get_service_by_name(service_name)
            if not result:
                return

            self.delete_resource(models.Service, result['id'])
        except Exception as e:
            raise exceptions.ResourceInUsing(models.Chain,
                                             service_name, e)

    def update_service(self, service_name, update_dict):
        resu = self.get_service_by_name(service_name)
        if not resu:
            return

        return self.update_resource(models.Service, resu['id'], update_dict)

    def list_service_by_attr(self, filter_dict):
        return self.list_resources_by_attr(models.Service, filter_dict)


class QueueMessageDao(BaseDAO):

    def get_queue_message_by_id(self, queue_message_id):
        return self.get_resource(models.QueueMessage, queue_message_id)

    def create_queue_message(self, chain_name, service_name, message_number):
        chain_dao = ChainDao()
        chain = chain_dao.get_chain_by_name(chain_name)
        if not chain:
            raise exceptions.ResourceNotFound(models.Chain, chain_name)

        service_dao = ServiceDao()
        service = service_dao.get_service_by_name(service_name)
        if not service:
            raise exceptions.ResourceNotFound(models.Service, service_name)

        now_time = datetime.datetime.now()
        now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
        queue_message_dict = {'chain_id': chain['id'], 'service_id': service['id'],
                              'message_number': message_number, 'timestamp': now_time}
        return self.create_resource(models.QueueMessage, queue_message_dict)
