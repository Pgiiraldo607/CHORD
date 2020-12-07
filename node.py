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
        #Se genera el primer NODO
        if self.FirstNODE:
            self.ServerLBi=self.ServerID
            self.ServerUBi=(1<<6)-1  #numero maximo de los bits del rango
            self.ServerLBd=0
            self.ServerUBd=self.ServerID
            self.SucessorNODE=host
            self.PredecessorNODE=host


            print('\n RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
            print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

        # Por acá se ingresa cuando ya hay al menos un Nodo en el anillo
        else:

            print('\n Insert the address and port of the node through which you want to enter the ring:\n')
            NodeConnect=input(' Example (IP:PORT) :  ')
            scktROOTNODE=context.socket(zmq.REQ) #para conectarme no solo con nodo ROOT sino con cualquier nodo


            while True:

                scktROOTNODE.connect('tcp://'+ NodeConnect)
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
                    scktROOTNODE.disconnect('tcp://'+ NodeConnect)
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
                    scktROOTNODE.disconnect('tcp://'+ NodeConnect)
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
                    scktROOTNODE.disconnect('tcp://'+NodeConnect)
                    scktROOTNODE.connect('tcp://'+self.PredecessorNODE)
                    scktROOTNODE.send_multipart([b'UpdateNode','updteSucessor'.encode(),self.ServerAddress.encode()])
                    scktROOTNODE.recv_string()
                    print('\n  RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
                    scktROOTNODE.disconnect('tcp://'+self.PredecessorNODE)
                    break


    def Ismember(self,Id,AddressSuc):
        ID=int(Id)
        if self.FirstNODE:   #NODO principalo o nodo con la union

            if self.PredecessorNODE == self.ServerAddress: # si es el único nodo en el anillo
                if (ID > self.ServerLBi and ID <= self.ServerUBi): #Valido si id nodo nuevo está en rango Izquierdo del nodo Principal

                    self.SucessorNODE=AddressSuc
                    self.PredecessorNODE=AddressSuc
                    auxLBi=self.ServerLBi
                    self.ServerLBi=ID
                    scktBINDNode.send_multipart([b'OLeftFNODE',self.ServerAddress.encode(),str(auxLBi).encode()])
                    print('\n NEW RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

                elif (ID>=self.ServerLBd and ID < self.ServerUBd): #Valido si id nodo nuevo está en rango Derecho del nodo Principal
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
                    scktBINDNode.send_multipart([b'ORightFNODE',self.ServerAddress.endoce(),str(auxLBi).encode(),str(auxUBi).encode(),str(auxLBd).encode()])
                    print('\n NEW RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))
                else:
                    Print('No se pa que hice esto, no es necesario')
                    #send_multipart([b'Not Member',self.SucessorNODE.encode()])

            else: #Si el  NODO principal no es el unico nodo en el anillo
                if (ID > self.ServerLBi and ID <= self.ServerUBi):
                    auxPredecessor=self.PredecessorNODE
                    self.PredecessorNODE=AddressSuc
                    auxLBi=self.ServerLBi
                    self.ServerLBi=ID
                    scktBINDNode.send_multipart([b'OLeftFNODE',self.ServerAddress.encode(),str(auxLBi).encode(),auxPredecessor.encode()])
                    print('\n NEW RESPONSABILITY RANGE:  ({},{}] U [{},{}]'.format(self.ServerLBi,self.ServerUBi,self.ServerLBd,self.ServerUBd))
                    print("\n ServerAddress --> {},\n Sucessor --> {},\n Predecesor --> {}".format(self.ServerAddress,self.SucessorNODE,self.PredecessorNODE))

                elif (ID>=self.ServerLBd and ID < self.ServerUBd):

                    print('\n NEW RESPONSABILITY RANGE:  ({},{}]'.format(self.ServerLBd,self.ServerUBd))

        else:
            if (ID > self.ServerLBd and ID < self.ServerUBd):
                auxPredecessor=self.PredecessorNODE
                self.PredecessorNODE=AddressSuc
                auxLBd=self.ServerLBd
                self.ServerLBd=ID
                scktBINDNode.send_multipart(['MemberFound'])

    def UpdateNode(self,ItemUpdate,SucessorNdIN):
        self.SucessorNODE=SucessorNdIN
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
        print (" NODE Waiting Request")
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
            MyNode.UpdateNode(ID,ItemUdate,ValueUpdate)

def main():

    ConnectNODE()
    RunningServer()

if __name__ == "__main__":
    main()
