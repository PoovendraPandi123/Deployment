import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
import yagmail
import logging

logger = logging.getLogger("vendor_reconciliation")

def send_mail_to_vendor(receiver_email):
    try:
        user = 'poovendrapandi.gb@gmail.com'
        app_password = "alsc nyrx kaqf tjzh"
        to = receiver_email

        user_name = to.split("@")[0].replace(".", " ")
        # sender_name = sender_first_name + " " + sender_last_name + ","

        # body = """<pre>Dear """ + user_name + """,""" + """\n\t""" + """Request you to Kindly review and approve the following Invoices through the link given below """ + """ \n\t\t\t""" + """<a href='""" + url + """'>Click Here</a></pre>""" + """\n\n""" + """Thanks and Regards,""" + """\n""" + sender_name + """\n""" + """vendorpayments@teamlease.com"""
        body = """
            Dear Shreejee Structural Steel Pvt Ltd,
                Our GL Balance is Rs.8746597. Kindly revert back the gl statement of your book to perform recon for this cycle.

            Thanks and Regards,
            vendorrecon@adventbizsolutions.com
        """
        subject = "Vendor Recon Shreejee Structural Steel Pvt Ltd" + " " + str(datetime.datetime.today())
        content = [body]

        with yagmail.SMTP(user, app_password) as yag:
            yag.send(to, subject, content)
            # print("Email Sent Successfully")

        return True

    except Exception as e:
        print(e)
        logger.error("Error in Sending Mail to the Vendor", exc_info=False)
        return False