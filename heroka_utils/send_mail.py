#  ======================================================================================
#! /usr/bin/env python
# Filename: send_mail.py
# Description: Generic mail() functionality defined to be used by different modules
# Author: Henin Karkada <henin.karkada@namastecredit.com>
# Python Environment - Python2/Python3
# Usage: Used as a module
# ========================================================================================

# Standard Modules
import smtplib

def mail(error_msg):
    """
    Mail to be sent to Engineering/Support team on case of critical errors to be acted upon
    :param error_msg: Error msg generated
    :return: None
    """

    # recipient = 'henin.roland@gmail.com'
    # cc = ['henin.roland@gmail.com', 'henin.karkada@namastecredit.com']
    # bcc = ['henin.karkada@namastecredit.com']
    # sender = 'henin.karkada@namastecredit.com'
    # subject = "[No Reply:] Notification email to the Engineering team!!!"
    # body = """Error: {}.
    #                This is an automated email to the team.""".format(error_msg)
    # message = """From: {}
    # To: {}
    # CC: {}
    # Subject: {}
    #
    # {}""".format(sender, recipient, cc, subject, body)
    #
    # sender = [sender] + cc + bcc
    # try:
    #     server = smtplib.SMTP('smtp.gmail.com', 587)
    #     # server.set_debuglevel(1)
    #     server.starttls()
    #     server.sendmail(sender, recipient, message)
    # except smtplib.SMTPException as error:
    #     pass
    #     # print(error)
    # finally:
    #     server.quit()
    print("Notification sent to the team!!! {} ".format(error_msg))