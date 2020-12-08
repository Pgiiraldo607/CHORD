# -*- coding: utf-8 -*-
"""
Created on Mon Nov 09 13:45:50 2020

@author: AndresGiraldo
"""

#import Range
import json
import zmq
import os
import getpass
import hashlib


Svr_ip=input('Insert NODE Ip Address: ')  #Ip del Servidor
Svr_port=input('Insert NODE Port: ') #Puerto del servidor para conexion con el cliente
Svr_id=input('Insert NODE Identification: ') #Ip para la conexion con el Proxy
First_NODE=input('Is the FIRST NOde ?  ( y / n) :  ')
host='tcp://'+Svr_ip+':'+Svr_port  #ip y puerto para conexion de los clientes y otros nodos
context = zmq.Context()
scktBINDNode = context.socket(zmq.REP)
scktBINDNode.bind("tcp://*:"+Svr_port) #socket de conexion con los clientes

SrvUrl='SERVER-FILES/'
MyNode=None

class Node:

    def __init__(self, Input_ID, Input_Address,Input_FNODE=False):

        self.ServerAddress = Input_Address
        self.ServerID = Input_ID
        self.FirstNODE = Input_FNODE
        self.SucessorNODE='sus'
        self.PredecessorNODE='Pred'
        self.ServerLBi=0
        self.ServerUBi=0
        self.ServerLBd=0
        self.ServerUBd=0

    #def printNeighboor(self):
    #    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

    def printNOde(self):
        print("\n ServerAddress --> {}\n ServerID --> {}\n FirstNODE --> {}".format(self.ServerAddress,self.ServerID,self.FirstNODE))


    def EntrytoRing(self):

        if self.FirstNODE:
            self.ServerLBi=self.ServerID
            self.ServerUBi=(1<<6)-1  #numero maximo de los bits del rango
            self.ServerLBd=0
            self.ServerUBd=self.ServerID
            self.SucessorNODE=host
            self.PredecessorNODE=host


            print('\n RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
            print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

        # Por acá se ingresa cuando ya hay al menos un Nodo en el anillo #Se genera el primer NODO
        else: #proceso para ingresar nodos adicionales

            print('\n Insert the address and port of the node through which you want to enter the ring:\n')
            ConnectTO=input(' Example (IP:PORT) :  ')
            NodeConnect='tcp://'+ConnectTO
            scktROOTNODE=context.socket(zmq.REQ) #para conectarme no solo con nodo ROOT sino con cualquier nodo


            while True:

                scktROOTNODE.connect(NodeConnect)
                print(NodeConnect)
                scktROOTNODE.send_multipart([b'Ismember',str(self.ServerID).encode(),self.ServerAddress.encode()])
                answ=scktROOTNODE.recv_multipart()

                if answ[0].decode()=='OLeftFNODE': #Si viene del rango Izq del unico nodo del anillo
                    #
                    #answ[0] opcion para dar ver como modificar el nodo nuevo
                    #answ[1] ip para sucesor y predecesor cuando solo hay un unico nodo en el anillo
                    #answ[2] Limite inferior Izquierdo que viene del nodo contactado para generar el rango
                    #
                    self.SucessorNODE=answ[1].decode()
                    self.PredecessorNODE=answ[1].decode()
                    self.ServerLBd=int(answ[2].decode())
                    self.ServerUBd=int(self.ServerID)
                    print('\n  RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
                    scktROOTNODE.disconnect(NodeConnect)
                    break


                elif answ[0].decode() == 'ORightFNODE': #Si viene del rango der. del unico nodo del anillo
                    #
                    #answ[0] opcion para dar ver como modificar el nodo nuevo
                    #answ[1] ip para sucesor y predecesor cuando solo hay un unico nodo en el anillo
                    #answ[2] Limite inferior Izquierdo que viene del nodo contactado para generar el rango
                    #answ[3] Limite superior Izquierdo que viene del nodo contactado para generar el rango
                    #answ[4] Limite inferior derecho que viene del nodo contactado para generar el rango
                    #
                    self.SucessorNODE=answ[1].decode()
                    self.PredecessorNODE=answ[1].decode()
                    self.ServerLBi=int(answ[2].decode())
                    self.ServerUBi=int(answ[3].decode())
                    self.ServerLBd=int(answ[4].decode())
                    self.ServerUBd=self.ServerID
                    self.FirstNODE=True
                    print('\n RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
                    scktROOTNODE.disconnect(NodeConnect)
                    break

                elif answ[0].decode() == 'LeftFNODE':
                    #
                    #answ[0] opcion para dar ver como modificar el nodo nuevo
                    #answ[1] ip para sucesor
                    #answ[2] Limite inferior Izquierdo que viene del nodo contactado para generar el rango
                    #answ[3] Ip Para Predecesor
                    #
                    self.SucessorNODE=answ[1].decode()
                    self.PredecessorNODE=answ[3].decode()
                    self.ServerLBd=int(answ[2].decode())
                    self.ServerUBd=int(self.ServerID)
                    scktROOTNODE.disconnect(NodeConnect)
                    scktROOTNODE.connect(self.PredecessorNODE)
                    scktROOTNODE.send_multipart([b'UpdateNode','updteSucessor'.encode(),self.ServerAddress.encode()])
                    scktROOTNODE.recv_string()
                    print('\n  RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
                    scktROOTNODE.disconnect(self.PredecessorNODE)
                    break  #Si viene del rango Izq del Nodo union del anillo cuando hay mas nodos

                elif answ[0].decode() == 'RightFNODE':
                    #
                    #answ[0] opcion para dar ver como modificar el nodo nuevo
                    #answ[1] ip para sucesor
                    #answ[2] ip Para Predecesor
                    #answ[3] Limite inferior Izquierdo que viene del nodo contactado para generar el rango
                    #answ[4] Limite superior Izquierdo que viene del nodo contactado para generar el rango
                    #answ[5] Limite inferior derecho que viene del nodo contactado para generar el rango
                    #
                    self.SucessorNODE=answ[1].decode()
                    self.PredecessorNODE=answ[2].decode()
                    self.ServerLBi=int(answ[3].decode())
                    self.ServerUBi=int(answ[4].decode())
                    self.ServerLBd=int(answ[5].decode())
                    self.ServerUBd=self.ServerID
                    self.FirstNODE=True
                    scktROOTNODE.disconnect(NodeConnect)
                    scktROOTNODE.connect(self.PredecessorNODE)
                    scktROOTNODE.send_multipart([b'UpdateNode','updteSucessor'.encode(),self.ServerAddress.encode()])
                    scktROOTNODE.recv_string()
                    print('\n RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
                    scktROOTNODE.disconnect(self.PredecessorNODE)
                    break #Si Viene del Rango Der del Nodo union del anillo cuando hay mas nodos

                elif answ[0].decode() == 'RegularNODE':
                    #
                    #answ[1] IP para SUCESOR
                    #answ[2] IP para PREDECESOR
                    #answ[3] Limite Inferior derecho para aramar Rango
                    #
                    self.SucessorNODE= answ[1].decode()
                    self.PredecessorNODE= answ[2].decode()
                    self.ServerLBd= int(answ[3].decode())
                    self.ServerUBd= self.ServerID
                    scktROOTNODE.disconnect(NodeConnect)
                    scktROOTNODE.connect(self.PredecessorNODE)
                    scktROOTNODE.send_multipart([b'UpdateNode','updteSucessor'.encode(),self.ServerAddress.encode()])
                    scktROOTNODE.recv_string()
                    print('\n  RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
                    scktROOTNODE.disconnect(self.PredecessorNODE)
                    break

                elif answ[0].decode() == 'NotMember': # Cuando el Nodo Union cuando hay mas nodos no es el responsable del Id del nuevo nodo
                    scktROOTNODE.disconnect(NodeConnect)
                    NodeConnect=answ[1].decode()



    def Ismember(self,Id,AddressSuc):
        ID=int(Id)
        if self.FirstNODE:   #NODO principalo o nodo con la union

            print('\n ip del servido r--- {} \n SUCESOR ANTES DE ACTUALIZAR --- {} \n PREDECESOR ANTES DE ACTUALIZAR --- {}'.format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
            if self.PredecessorNODE == self.ServerAddress: # si es el único nodo en el anillo

                if (ID > self.ServerLBi and ID <= self.ServerUBi): #Valido si id nodo nuevo está en rango Izquierdo del nodo Principal
                    print('\n UNICO NODO EN EL ANILLO LADO IZQ\n')
                    self.SucessorNODE=AddressSuc
                    self.PredecessorNODE=AddressSuc
                    auxLBi=self.ServerLBi
                    self.ServerLBi=ID
                    scktBINDNode.send_multipart([b'OLeftFNODE',self.ServerAddress.encode(),str(auxLBi).encode()])
                    print('\n NEW RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

                elif (ID>=self.ServerLBd and ID < self.ServerUBd): #Valido si id nodo nuevo está en rango Derecho del nodo Principal
                    print('\n UNICO NODO EN EL ANILLO LADO DERECHO\n')
                    self.FirstNODE=False
                    self.SucessorNODE=AddressSuc
                    self.PredecessorNODE=AddressSuc
                    auxLBi=self.ServerLBi
                    auxUBi=self.ServerUBi
                    auxLBd=self.ServerLBd
                    self.ServerLBi = 0
                    self.ServerUBi = 0
                    self.ServerLBd = ID
                    self.ServerUBd = self.ServerID
                    scktBINDNode.send_multipart([b'ORightFNODE',self.ServerAddress.encode(),str(auxLBi).encode(),str(auxUBi).encode(),str(auxLBd).encode()])
                    print('\n NEW RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))


            else: #Si el  NODO principal no es el unico nodo en el anillo
                if (ID > self.ServerLBi and ID <= self.ServerUBi):
                    print('\n HAY MAS NODO EN EL ANILLO LADO IZQ\n')
                    auxPredecessor=self.PredecessorNODE
                    self.PredecessorNODE=AddressSuc
                    auxLBi=self.ServerLBi
                    self.ServerLBi=ID
                    scktBINDNode.send_multipart([b'LeftFNODE',self.ServerAddress.encode(),str(auxLBi).encode(),auxPredecessor.encode()])
                    print('\n NEW RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

                elif (ID>=self.ServerLBd and ID < self.ServerUBd):

                    self.FirstNODE=False
                    auxPredecessor=self.PredecessorNODE
                    self.PredecessorNODE=AddressSuc
                    auxLBi=self.ServerLBi
                    auxUBi=self.ServerUBi
                    auxLBd=self.ServerLBd
                    self.ServerLBi = 0
                    self.ServerUBi = 0
                    self.ServerLBd = ID
                    self.ServerUBd = self.ServerID
                    scktBINDNode.send_multipart([b'RightFNODE',self.ServerAddress.encode(),auxPredecessor.encode(),str(auxLBi).encode(),str(auxUBi).encode(),str(auxLBd).encode()])
                    print('\n NEW RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

                else:
                    print('Member Not Found')
                    scktBINDNode.send_multipart([b'NotMember',self.SucessorNODE.encode()])

        else:  #Si estamos preguntando a un nodo que no es el principal
            if (ID > self.ServerLBd and ID < self.ServerUBd):
                auxPredecessor=self.PredecessorNODE
                self.PredecessorNODE=AddressSuc
                auxLBd=self.ServerLBd
                self.ServerLBd=ID
                scktBINDNode.send_multipart([b'RegularNODE', self.ServerAddress.encode(), auxPredecessor.encode(), str(auxLBd).encode()])
                print('\n  RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
                print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

            else:
                print('Member Not Found')
                scktBINDNode.send_multipart([b'NotMember',self.SucessorNODE.encode()])

    def UpdateNode(self,ItemUpdate,SucessorNdIN):
        self.SucessorNODE=SucessorNdIN
        if self.FirstNODE:
            print('\n RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
            print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
        else:
            print('\n  RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
            print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
        scktBINDNode.send_string('Sucessor Updated')

def ConnectNODE():
    global MyNode
    if First_NODE == 'y':
        NewNode=Node(int(Svr_id),host,True)
        NewNode.printNOde()
        NewNode.EntrytoRing()
        MyNode=NewNode


    else:

        NewNode=Node(int(Svr_id),host)
        NewNode.printNOde()
        NewNode.EntrytoRing()
        MyNode=NewNode

    print('\n    NODE CONNECTED    \n')

def RunningServer():
    while True:

        #  Wait for next request from client an Storage Svr
        print (" NODE Waiting Request\n")
        OpcREQ = scktBINDNode.recv_multipart()
        Opc=OpcREQ[0]
        print('Option Selected: ' + Opc.decode())

        if   Opc == b"Ismember":

            ID = OpcREQ[1].decode()
            AddressSuc = OpcREQ[2].decode()
            MyNode.Ismember(ID,AddressSuc)

        elif   Opc == b"UpdateNode":
            #
            #OpcREQ[1] atributo a actualizar del nodo
            #OpcREQ[2] Valor para actualizar al nodo

            ItemUdate = OpcREQ[1].decode()
            ValueUpdate = OpcREQ[2].decode()
            MyNode.UpdateNode(ItemUdate,ValueUpdate)
        

def main():

    ConnectNODE()
    RunningServer()

if __name__ == "__main__":
    main()
