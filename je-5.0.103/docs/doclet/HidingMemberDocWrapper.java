/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.MemberDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;

class HidingMemberDocWrapper extends HidingWrapper implements MemberDoc {

    public HidingMemberDocWrapper(MemberDoc memdoc, Map mapWrappers) {
        super(memdoc, mapWrappers);
    }

    private MemberDoc _getMemberDoc() {
        return (MemberDoc)getWrappedObject();
    }

    public boolean isSynthetic() {
        return _getMemberDoc().isSynthetic();
    }

    public AnnotationDesc[] annotations() {
        return _getMemberDoc().annotations();
    }

    public boolean isEnum() {
        return _getMemberDoc().isEnum();
    }

    public boolean isAnnotationTypeElement() {
        return _getMemberDoc().isAnnotationTypeElement();
    }

    public boolean isEnumConstant() {
        return _getMemberDoc().isEnumConstant(); 
    }
    
    public String commentText() {
        return _getMemberDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getMemberDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else {
            return _getMemberDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getMemberDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getMemberDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getMemberDoc().inlineTags());
    }

    public boolean isClass() {
        return _getMemberDoc().isClass(); 
    }

    public boolean isConstructor() {
        return _getMemberDoc().isConstructor();
    }

    public boolean isError() {
        return _getMemberDoc().isError();
    }

    public boolean isException() {
        return _getMemberDoc().isException();
    }

    public boolean isField() {
        return _getMemberDoc().isField();
    }

    public boolean isIncluded() {
        return _getMemberDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getMemberDoc().isInterface();
    }

    public boolean isMethod() {
        return _getMemberDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getMemberDoc().isOrdinaryClass();
    }

    public String name() {
        return _getMemberDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getMemberDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getMemberDoc().setRawCommentText(szText); 
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getMemberDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getMemberDoc().tags(szTagName));
    }
  
    public SourcePosition position() {
        return _getMemberDoc().position();
    }

    public boolean isAnnotationType() {
        return _getMemberDoc().isAnnotationType();
    }
   
    public ClassDoc containingClass() {
        return (ClassDoc)wrapOrHide(_getMemberDoc().containingClass());
    }
    
    public PackageDoc containingPackage() {
        return (PackageDoc)wrapOrHide(_getMemberDoc().containingPackage());
    }

    public boolean isFinal() {
        return _getMemberDoc().isFinal();
    }

    public boolean isPackagePrivate() {
        return _getMemberDoc().isPackagePrivate();
    }

    public boolean isPrivate() {
        return _getMemberDoc().isPrivate();
    }

    public boolean isProtected() {
        return _getMemberDoc().isProtected();
    }

    public boolean isPublic() {
        return _getMemberDoc().isPublic();
    }

    public boolean isStatic() {
        return _getMemberDoc().isStatic();
    }

    public int modifierSpecifier() {
        return _getMemberDoc().modifierSpecifier();
    }

    public String modifiers() {
        return _getMemberDoc().modifiers();
    }

    public String qualifiedName() {
        return _getMemberDoc().qualifiedName();
    }
}
